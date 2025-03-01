// src/services/directDonationMonitor.js
const ethers = require('ethers');
const db = require('../db');
const { providers, contracts, NETWORKS } = require('./blockchain');
const { createLogger, format, transports } = require('winston');

/**
 * Logger configuration - improved format and detail
 */
const logger = createLogger({
  level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
  format: format.combine(
    format.timestamp(),
    format.errors({ stack: true }),
    format.json()
  ),
  defaultMeta: { service: 'direct-donation-monitor' },
  transports: [
    new transports.Console({
      format: format.combine(
        format.colorize(),
        format.printf(({ level, message, timestamp, service, ...meta }) => {
          // Include metadata for all log levels in development, only errors in production
          const isImportant = level === 'error' || level === 'warn';
          const metaStr = (isImportant || process.env.NODE_ENV !== 'production') && 
            Object.keys(meta).length ? JSON.stringify(meta) : '';
          return `${timestamp} [${service}] ${level}: ${message} ${metaStr}`;
        })
      )
    }),
    new transports.File({ filename: 'logs/direct-donations.log' })
  ]
});

// Configuration constants
const CONFIG = {
  // Monitoring intervals
  INITIAL_CHECK_DELAY_MS: 5000,      // Initial check after 5 seconds
  BALANCE_CHECK_INTERVAL_MS: 30000,  // Check balances every 30 seconds
  TX_CHECK_INTERVAL_MS: 60000,       // Check transactions every 1 minute
  STUCK_TX_CHECK_INTERVAL_MS: 120000, // Check for stuck transactions every 2 minutes
  
  // Donation parameters
  MIN_DONATION_MATIC: 0.3,           // Minimum donation amount in MATIC
  GAS_RESERVE_PERCENT: 35,           // Percentage of balance to reserve for gas
  GAS_PRICE_BOOST: 130,              // Percentage of network gas price to use (130% = higher priority)
  GAS_LIMIT_BUFFER: 150,             // Percentage buffer on estimated gas (150% = 50% extra)
  
  // Timeouts and limits
  RPC_RETRY_ATTEMPTS: 3,             // Number of times to retry failed RPC calls
  RPC_RETRY_DELAY_MS: 1000,          // Base delay between retries (increases with each attempt)
  TX_TIMEOUT_MINUTES: 15,            // How long to wait before considering a transaction stuck
  TX_FINAL_TIMEOUT_MINUTES: 30,      // How long to wait before considering a transaction failed
  MIN_BLOCKS_CONFIRMATIONS: 3,       // Number of block confirmations to wait for
  
  // Cooldown to prevent duplicate processing
  WALLET_COOLDOWN_MINUTES: 3,        // Minimum time between processing the same wallet
  
  // Stuck transaction handling
  STUCK_TX_RETRY_COUNT: 3,           // Number of times to retry a stuck transaction
  STUCK_TX_GAS_BOOST: 150            // Percentage increase for gas price on retry (150% = 50% more)
};

// Store for transaction tracking
const txTracker = {
  // Track retries for stuck transactions
  retryCount: new Map(),
  
  // Add a transaction to track
  addTransaction(txHash, donationId) {
    this.retryCount.set(txHash, {
      donationId,
      count: 0,
      timestamp: Date.now()
    });
  },
  
  // Get transactions that might be stuck
  getStuckTransactions(olderThanMinutes) {
    const now = Date.now();
    const threshold = olderThanMinutes * 60 * 1000;
    
    return Array.from(this.retryCount.entries())
      .filter(([_, data]) => (now - data.timestamp) > threshold && data.count < CONFIG.STUCK_TX_RETRY_COUNT)
      .map(([txHash, data]) => ({
        txHash,
        donationId: data.donationId,
        count: data.count
      }));
  },
  
  // Increment retry count for a transaction
  incrementRetryCount(txHash) {
    const data = this.retryCount.get(txHash);
    if (data) {
      data.count++;
      data.timestamp = Date.now();
      this.retryCount.set(txHash, data);
      return data.count;
    }
    return 0;
  },
  
  // Remove a transaction from tracking
  removeTransaction(txHash) {
    this.retryCount.delete(txHash);
  },
  
  // Clean up old transactions
  cleanupOldTransactions() {
    const now = Date.now();
    const threshold = CONFIG.TX_FINAL_TIMEOUT_MINUTES * 60 * 1000;
    
    for (const [txHash, data] of this.retryCount.entries()) {
      if ((now - data.timestamp) > threshold) {
        this.retryCount.delete(txHash);
      }
    }
  }
};

/**
 * Helper function to retry failed RPC calls with exponential backoff
 */
async function withRetry(operation, name = 'RPC call') {
  let lastError;
  
  for (let attempt = 1; attempt <= CONFIG.RPC_RETRY_ATTEMPTS; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      
      // Only log if it's the last attempt or significant error
      if (attempt === CONFIG.RPC_RETRY_ATTEMPTS || error.code !== 'SERVER_ERROR') {
        logger.debug(`${name} failed (attempt ${attempt}/${CONFIG.RPC_RETRY_ATTEMPTS}): ${error.message}`);
      }
      
      // Exponential backoff before retrying
      if (attempt < CONFIG.RPC_RETRY_ATTEMPTS) {
        await new Promise(resolve => 
          setTimeout(resolve, CONFIG.RPC_RETRY_DELAY_MS * Math.pow(2, attempt - 1))
        );
      }
    }
  }
  
  throw lastError;
}

/**
 * Main entry point - starts the donation monitoring service
 */
async function monitorDirectDonations() {
  logger.info('Starting direct donation monitor');
  
  try {
    // Find the main chain configuration
    const mainChain = Object.keys(NETWORKS).find(network => NETWORKS[network].isMain);
    if (!mainChain || !providers[mainChain] || !contracts[mainChain]) {
      throw new Error('Main chain provider or contract not available');
    }
    
    const provider = providers[mainChain];
    const mainContract = contracts[mainChain];
    
    logger.info(`Using ${mainChain} as the main chain for donations`);
    
    // Run initial checks after a short delay to ensure DB connection is ready
    setTimeout(async () => {
      try {
        logger.info('Running initial balance and transaction checks');
        await monitorWalletBalances(provider);
        await processPendingDonations(provider, mainContract);
        await updateTransactionStatus(provider);
      } catch (error) {
        logger.error('Error during initial checks:', error);
      }
    }, CONFIG.INITIAL_CHECK_DELAY_MS);
    
    // Set up recurring monitoring intervals
    
    // 1. Monitor wallet balances - more frequent checks
    setInterval(async () => {
      try {
        await monitorWalletBalances(provider);
      } catch (error) {
        logger.error('Error monitoring wallet balances:', error);
      }
    }, CONFIG.BALANCE_CHECK_INTERVAL_MS);
    
    // 2. Process pending donations
    setInterval(async () => {
      try {
        await processPendingDonations(provider, mainContract);
      } catch (error) {
        logger.error('Error processing pending donations:', error);
      }
    }, CONFIG.BALANCE_CHECK_INTERVAL_MS);
    
    // 3. Check transaction status
    setInterval(async () => {
      try {
        await updateTransactionStatus(provider);
      } catch (error) {
        logger.error('Error updating transaction status:', error);
      }
    }, CONFIG.TX_CHECK_INTERVAL_MS);
    
    // 4. Handle stuck transactions
    setInterval(async () => {
      try {
        await handleStuckTransactions(provider, mainContract);
        txTracker.cleanupOldTransactions();
      } catch (error) {
        logger.error('Error handling stuck transactions:', error);
      }
    }, CONFIG.STUCK_TX_CHECK_INTERVAL_MS);
    
    logger.info('Direct donation monitor started successfully');
    return true;
  } catch (error) {
    logger.error('Failed to start direct donation monitor:', error);
    return false;
  }
}

/**
 * Step 1: Monitor wallet balances and create pending donations
 * when significant deposits are detected.
 */
async function monitorWalletBalances(provider) {
  try {
    // Get all campaign wallets
    const walletResult = await db.query(`
      SELECT 
        w.campaign_id, 
        w.wallet_address,
        (SELECT MAX(created_at) FROM direct_donations 
         WHERE wallet_address = w.wallet_address AND status != 'failed') as last_donation_time
      FROM campaign_wallets w
    `);
    
    if (walletResult.rows.length === 0) {
      return;
    }
    
    logger.debug(`Checking balances for ${walletResult.rows.length} campaign wallets`);
    
    for (const wallet of walletResult.rows) {
      try {
        // Check if this wallet has a donation in progress
        const activeDonation = await db.query(`
          SELECT id FROM direct_donations 
          WHERE wallet_address = $1 AND status IN ('pending', 'processing')
          LIMIT 1
        `, [wallet.wallet_address]);
        
        if (activeDonation.rows.length > 0) {
          // Skip wallets with active donations
          continue;
        }
        
        // Check cooldown period to prevent duplicate processing
        if (wallet.last_donation_time) {
          const cooldownMinutes = CONFIG.WALLET_COOLDOWN_MINUTES;
          const cooldownExpiry = new Date(wallet.last_donation_time);
          cooldownExpiry.setMinutes(cooldownExpiry.getMinutes() + cooldownMinutes);
          
          if (new Date() < cooldownExpiry) {
            // Still in cooldown period, skip this wallet
            continue;
          }
        }
        
        // Check current balance
        const balance = await withRetry(
          () => provider.getBalance(wallet.wallet_address),
          `Get balance for ${wallet.wallet_address}`
        );
        
        const balanceEther = Number(ethers.formatEther(balance));
        
        // Skip if balance is below minimum donation threshold
        if (balanceEther < CONFIG.MIN_DONATION_MATIC) {
          continue;
        }
        
        // Get the most recent completed donation for this wallet
        const previousDonation = await db.query(`
          SELECT amount FROM direct_donations 
          WHERE wallet_address = $1 AND status = 'completed'
          ORDER BY processed_at DESC LIMIT 1
        `, [wallet.wallet_address]);
        
        const previousAmount = previousDonation.rows.length > 0 ? 
          ethers.parseEther(previousDonation.rows[0].amount) : 0n;
        
        // Only create a new donation if balance is significantly higher
        // than the previous donation amount (add a small buffer to account for dust)
        const minSignificantChange = ethers.parseEther("0.05");
        
        if (balance <= previousAmount + minSignificantChange) {
          // Balance hasn't changed enough to warrant a new donation
          continue;
        }
        
        logger.info(`New donation detected: ${ethers.formatEther(balance)} MATIC in wallet ${wallet.wallet_address.substring(0, 8)}... for campaign ${wallet.campaign_id}`);
        
        // Create a new pending donation
        await db.query(`
          INSERT INTO direct_donations (
            campaign_id, wallet_address, amount, status, source_tx_hash, created_at
          ) VALUES ($1, $2, $3, $4, $5, NOW())
        `, [
          wallet.campaign_id,
          wallet.wallet_address,
          ethers.formatEther(balance),
          'pending',
          `balance-${Date.now()}`
        ]);
      } catch (error) {
        logger.error(`Error monitoring wallet ${wallet.wallet_address}:`, error);
      }
    }
  } catch (error) {
    logger.error('Error monitoring wallet balances:', error);
  }
}

/**
 * Step 2: Process pending donations by sending transactions
 * to the main contract.
 */
async function processPendingDonations(provider, mainContract) {
  try {
    // Get pending donations sorted by creation time (oldest first)
    const pendingDonations = await db.query(`
      SELECT 
        d.id, 
        d.campaign_id, 
        d.wallet_address, 
        d.amount, 
        w.private_key
      FROM direct_donations d
      JOIN campaign_wallets w ON d.wallet_address = w.wallet_address
      WHERE d.status = 'pending'
      ORDER BY d.created_at ASC
    `);
    
    if (pendingDonations.rows.length === 0) {
      return;
    }
    
    logger.info(`Processing ${pendingDonations.rows.length} pending donations`);
    
    for (const donation of pendingDonations.rows) {
      try {
        // Double-check that the donation is still pending
        const currentStatus = await db.query(
          'SELECT status FROM direct_donations WHERE id = $1',
          [donation.id]
        );
        
        if (currentStatus.rows.length === 0 || currentStatus.rows[0].status !== 'pending') {
          // Donation no longer pending, skip it
          continue;
        }
        
        // Create wallet from private key
        const wallet = new ethers.Wallet(donation.private_key, provider);
        
        // Check for pending transactions from this wallet
        const pendingTxCount = await withRetry(
          () => provider.getTransactionCount(wallet.address, 'pending'),
          `Get pending tx count for ${wallet.address}`
        );
        
        const confirmedTxCount = await withRetry(
          () => provider.getTransactionCount(wallet.address, 'latest'),
          `Get confirmed tx count for ${wallet.address}`
        );
        
        if (pendingTxCount > confirmedTxCount) {
          logger.info(`Skipping donation ${donation.id}: wallet has ${pendingTxCount - confirmedTxCount} pending transactions`);
          continue;
        }
        
        // Check current wallet balance
        const currentBalance = await withRetry(
          () => provider.getBalance(wallet.address),
          `Get current balance for ${wallet.address}`
        );
        
        // Skip if balance is too low for a meaningful donation
        if (currentBalance < ethers.parseEther("0.2")) {
          logger.debug(`Skipping donation ${donation.id}: insufficient balance (${ethers.formatEther(currentBalance)} MATIC)`);
          continue;
        }
        
        // Calculate donation amount, reserving gas
        const gasReserve = currentBalance * BigInt(CONFIG.GAS_RESERVE_PERCENT) / BigInt(100);
        const donationAmount = currentBalance - gasReserve;
        
        if (donationAmount <= 0n) {
          logger.warn(`Donation ${donation.id} has insufficient funds after gas reserve`);
          await db.query(
            `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
            [donation.id]
          );
          continue;
        }
        
        // Get current gas estimate with a safe value for estimation
        const safeEstimationAmount = donationAmount;
        
        const gasEstimate = await withRetry(
          () => mainContract.connect(wallet).donate.estimateGas(
            donation.campaign_id,
            ethers.ZeroAddress,
            0,
            { value: safeEstimationAmount }
          ),
          `Estimate gas for donation ${donation.id}`
        );
        
        // Get current network gas prices
        const feeData = await withRetry(
          () => provider.getFeeData(),
          `Get fee data for donation ${donation.id}`
        );
        
        // Set higher gas price for faster confirmation
        const maxFeePerGas = (feeData.maxFeePerGas || feeData.gasPrice) * 
          BigInt(CONFIG.GAS_PRICE_BOOST) / BigInt(100);
        
        const maxPriorityFeePerGas = (feeData.maxPriorityFeePerGas || 
          (feeData.maxFeePerGas ? feeData.maxFeePerGas / 2n : feeData.gasPrice)) * 
          BigInt(CONFIG.GAS_PRICE_BOOST) / BigInt(100);
        
        // Add extra buffer to gas limit
        const gasLimit = gasEstimate * BigInt(CONFIG.GAS_LIMIT_BUFFER) / BigInt(100);
        
        logger.info(`Sending donation ${donation.id}: ${ethers.formatEther(donationAmount)} MATIC to campaign ${donation.campaign_id}`);
        
        // Send the transaction
        const tx = await withRetry(
          () => mainContract.connect(wallet).donate(
            donation.campaign_id,
            ethers.ZeroAddress,
            0,
            {
              value: donationAmount,
              maxFeePerGas,
              maxPriorityFeePerGas,
              gasLimit
            }
          ),
          `Send donation transaction for ${donation.id}`
        );
        
        logger.info(`Donation ${donation.id} transaction sent: ${tx.hash}`);
        
        // Add transaction to tracker
        txTracker.addTransaction(tx.hash, donation.id);
        
        // Update donation status
        await db.query(
          `UPDATE direct_donations SET status = 'processing', contract_tx_hash = $1 WHERE id = $2`,
          [tx.hash, donation.id]
        );
      } catch (error) {
        logger.error(`Error processing donation ${donation.id}:`, error);
        
        // Mark as failed if processing failed
        await db.query(
          `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
          [donation.id]
        );
      }
    }
  } catch (error) {
    logger.error('Error processing pending donations:', error);
  }
}

/**
 * Step 3: Update the status of processing donations
 * based on their transaction status on the blockchain.
 */
async function updateTransactionStatus(provider) {
  try {
    // Get all processing donations
    const processingDonations = await db.query(`
      SELECT 
        id, 
        contract_tx_hash, 
        EXTRACT(EPOCH FROM (NOW() - created_at)) as age_seconds
      FROM direct_donations 
      WHERE status = 'processing' AND contract_tx_hash IS NOT NULL
    `);
    
    if (processingDonations.rows.length === 0) {
      return;
    }
    
    for (const donation of processingDonations.rows) {
      try {
        if (!donation.contract_tx_hash) continue;
        
        // Check transaction receipt
        const receipt = await withRetry(
          () => provider.getTransactionReceipt(donation.contract_tx_hash),
          `Get transaction receipt for ${donation.contract_tx_hash}`
        ).catch(() => null); // Silently catch errors and return null
        
        if (receipt) {
          // Transaction was mined
          if (receipt.status === 1) {
            // Transaction successful
            logger.info(`Donation ${donation.id} confirmed successfully in tx ${donation.contract_tx_hash.substring(0, 10)}...`);
            
            // Remove from tracker
            txTracker.removeTransaction(donation.contract_tx_hash);
            
            await db.query(
              `UPDATE direct_donations SET status = 'completed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          } else {
            // Transaction failed
            logger.warn(`Donation ${donation.id} failed in tx ${donation.contract_tx_hash.substring(0, 10)}...`);
            
            // Remove from tracker
            txTracker.removeTransaction(donation.contract_tx_hash);
            
            await db.query(
              `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          }
        } else {
          // No receipt yet - check if it's final timeout
          const finalTimeoutSeconds = CONFIG.TX_FINAL_TIMEOUT_MINUTES * 60;
          
          if (donation.age_seconds > finalTimeoutSeconds) {
            logger.warn(`Donation ${donation.id} timed out after ${CONFIG.TX_FINAL_TIMEOUT_MINUTES} minutes`);
            
            // Remove from tracker
            txTracker.removeTransaction(donation.contract_tx_hash);
            
            await db.query(
              `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          }
        }
      } catch (error) {
        logger.error(`Error updating status for donation ${donation.id}:`, error);
      }
    }
  } catch (error) {
    logger.error('Error updating transaction status:', error);
  }
}

/**
 * Step 4: Handle stuck transactions by replacing them with
 * higher gas price transactions with the same nonce.
 */
async function handleStuckTransactions(provider, mainContract) {
  try {
    // Find transactions that might be stuck (older than TX_TIMEOUT_MINUTES)
    const stuckTxs = txTracker.getStuckTransactions(CONFIG.TX_TIMEOUT_MINUTES);
    
    if (stuckTxs.length === 0) {
      return;
    }
    
    logger.info(`Found ${stuckTxs.length} potentially stuck transactions`);
    
    for (const stuckTx of stuckTxs) {
      try {
        // Check if transaction already confirmed but just missed in our checks
        const receipt = await withRetry(
          () => provider.getTransactionReceipt(stuckTx.txHash),
          `Check receipt for stuck tx ${stuckTx.txHash}`
        ).catch(() => null);
        
        if (receipt) {
          // Transaction was actually mined, update its status
          logger.info(`Stuck transaction ${stuckTx.txHash.substring(0, 10)}... was actually mined`);
          
          // Remove from tracker
          txTracker.removeTransaction(stuckTx.txHash);
          
          // Update donation status based on receipt
          if (receipt.status === 1) {
            await db.query(
              `UPDATE direct_donations SET status = 'completed', processed_at = NOW() WHERE id = $1`,
              [stuckTx.donationId]
            );
          } else {
            await db.query(
              `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
              [stuckTx.donationId]
            );
          }
          
          continue;
        }
        
        // Get the donation details
        const donationResult = await db.query(`
          SELECT d.campaign_id, d.wallet_address, w.private_key
          FROM direct_donations d
          JOIN campaign_wallets w ON d.wallet_address = w.wallet_address
          WHERE d.id = $1 AND d.status = 'processing'
        `, [stuckTx.donationId]);
        
        if (donationResult.rows.length === 0) {
          // Donation no longer in processing state, remove from tracker
          txTracker.removeTransaction(stuckTx.txHash);
          continue;
        }
        
        const donation = donationResult.rows[0];
        
        // Get the original transaction
        const tx = await withRetry(
          () => provider.getTransaction(stuckTx.txHash),
          `Get stuck transaction ${stuckTx.txHash}`
        ).catch(() => null);
        
        if (!tx) {
          logger.warn(`Could not retrieve stuck transaction ${stuckTx.txHash.substring(0, 10)}...`);
          continue;
        }
        
        // Create wallet from private key
        const wallet = new ethers.Wallet(donation.private_key, provider);
        
        // Get the nonce of the stuck transaction
        const nonce = tx.nonce;
        
        // Get current gas prices
        const feeData = await withRetry(
          () => provider.getFeeData(),
          `Get fee data for stuck tx ${stuckTx.txHash}`
        );
        
        // Calculate higher gas price based on number of retries
        const retryCount = txTracker.incrementRetryCount(stuckTx.txHash);
        const boostMultiplier = CONFIG.STUCK_TX_GAS_BOOST + (retryCount * 20); // Increase by 20% more each retry
        
        const maxFeePerGas = (feeData.maxFeePerGas || feeData.gasPrice) * 
          BigInt(boostMultiplier) / BigInt(100);
        
        const maxPriorityFeePerGas = (feeData.maxPriorityFeePerGas || 
          (feeData.maxFeePerGas ? feeData.maxFeePerGas / 2n : feeData.gasPrice)) * 
          BigInt(boostMultiplier) / BigInt(100);
        
        logger.info(`Replacing stuck tx ${stuckTx.txHash.substring(0, 10)}... (retry ${retryCount}) with higher gas price`);
        
        // Send empty transaction with same nonce and higher gas price
        const replacementTx = await wallet.sendTransaction({
          to: wallet.address, // Send to self
          value: 0n,          // Zero value
          nonce: nonce,       // Same nonce as stuck tx
          maxFeePerGas,
          maxPriorityFeePerGas,
          gasLimit: 21000n    // Minimum gas for empty transaction
        });
        
        logger.info(`Replacement tx sent: ${replacementTx.hash}`);
        
        // Update the donation record with the new tx hash
        await db.query(
          `UPDATE direct_donations SET contract_tx_hash = $1 WHERE id = $2`,
          [replacementTx.hash, stuckTx.donationId]
        );
        
        // Add new transaction to tracker and remove old one
        txTracker.removeTransaction(stuckTx.txHash);
        txTracker.addTransaction(replacementTx.hash, stuckTx.donationId);
      } catch (error) {
        logger.error(`Error handling stuck transaction ${stuckTx.txHash}:`, error);
      }
    }
  } catch (error) {
    logger.error('Error handling stuck transactions:', error);
  }
}

module.exports = { monitorDirectDonations };