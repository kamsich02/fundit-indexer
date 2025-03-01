// src/services/directDonationMonitor.js
const ethers = require('ethers');
const db = require('../db');
const { providers, contracts, NETWORKS } = require('./blockchain');
const { createLogger, format, transports } = require('winston');

// Logger configuration
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
        format.printf(({ level, message, timestamp }) => {
          return `${timestamp} [direct-donation-monitor] ${level}: ${message}`;
        })
      )
    }),
    new transports.File({ filename: 'logs/direct-donations.log' })
  ]
});

// Configuration
const CONFIG = {
  MIN_DONATION_AMOUNT: 1.0, // Minimum donation in MATIC
  GAS_RESERVE_PERCENT: 10,  // Percentage to reserve for gas (reduced from 30%)
  GAS_RESERVE_MIN: 0.05,    // Minimum gas reserve in MATIC
  MAX_PENDING_CHECKS: 15,   // Max number of times to check a pending tx before replacing
  CHECK_INTERVAL_MS: 60000, // Check every minute
  GAS_PRICE_BOOST: 120      // 20% boost for gas price 
};

// Main function to start monitoring
async function monitorDirectDonations() {
  logger.info('Starting direct donation monitor');
  
  try {
    // Find the main chain
    const mainChain = Object.keys(NETWORKS).find(network => NETWORKS[network].isMain);
    if (!mainChain || !providers[mainChain] || !contracts[mainChain]) {
      throw new Error('Main chain provider or contract not available');
    }
    
    const provider = providers[mainChain];
    const mainContract = contracts[mainChain];
    
    logger.info(`Using ${mainChain} as the main chain for donations`);
    
    // Start the monitoring loop
    setInterval(() => checkWalletsAndProcess(provider, mainContract), CONFIG.CHECK_INTERVAL_MS);
    
    // Initial check
    checkWalletsAndProcess(provider, mainContract);
    
    return true;
  } catch (error) {
    logger.error('Failed to start direct donation monitor:', error);
    return false;
  }
}

// Main monitoring loop
async function checkWalletsAndProcess(provider, mainContract) {
  try {
    logger.debug('Starting wallet check cycle');
    
    // Get all campaign wallets
    const wallets = await db.query(`
      SELECT 
        campaign_id, 
        wallet_address, 
        private_key
      FROM campaign_wallets
    `);
    
    if (wallets.rows.length === 0) {
      logger.debug('No campaign wallets found');
      return;
    }
    
    logger.debug(`Checking ${wallets.rows.length} campaign wallets`);
    
    // Process each wallet
    for (const wallet of wallets.rows) {
      try {
        await processWallet(wallet, provider, mainContract);
      } catch (error) {
        logger.error(`Error processing wallet ${wallet.wallet_address}:`, error);
      }
    }
    
    logger.debug('Wallet check cycle completed');
  } catch (error) {
    logger.error('Error in check and process cycle:', error);
  }
}

// Process a single wallet
async function processWallet(wallet, provider, mainContract) {
  // Check for pending donations
  const pendingDonation = await db.query(`
    SELECT 
      id, 
      contract_tx_hash, 
      check_count
    FROM direct_donations 
    WHERE wallet_address = $1 AND status = 'pending'
    LIMIT 1
  `, [wallet.wallet_address]);
  
  // If there's a pending donation, check its status
  if (pendingDonation.rows.length > 0) {
    const donation = pendingDonation.rows[0];
    return await handlePendingDonation(donation, wallet, provider, mainContract);
  }
  
  // No pending donations, check balance and create new donation if needed
  return await checkBalanceAndCreateDonation(wallet, provider, mainContract);
}

// Handle pending donation
async function handlePendingDonation(donation, wallet, provider, mainContract) {
  const txHash = donation.contract_tx_hash;
  
  // If no transaction hash yet, it's a new pending donation waiting for processing
  if (!txHash) {
    return await sendDonationTransaction(donation.id, wallet, provider, mainContract);
  }
  
  // Check transaction status
  try {
    const receipt = await provider.getTransactionReceipt(txHash);
    
    // Transaction confirmed
    if (receipt) {
      logger.info(`Transaction ${txHash.substring(0, 10)}... confirmed with status: ${receipt.status}`);
      
      // Update donation status based on transaction status
      const newStatus = receipt.status === 1 ? 'completed' : 'failed';
      await db.query(
        `UPDATE direct_donations SET status = $1, processed_at = NOW() WHERE id = $2`,
        [newStatus, donation.id]
      );
      return;
    }
    
    // Transaction still pending, increment check count
    const newCheckCount = (donation.check_count || 0) + 1;
    await db.query(
      `UPDATE direct_donations SET check_count = $1 WHERE id = $2`,
      [newCheckCount, donation.id]
    );
    
    // If we've checked too many times, try to replace the transaction
    if (newCheckCount >= CONFIG.MAX_PENDING_CHECKS) {
      logger.warn(`Transaction ${txHash.substring(0, 10)}... stuck, attempting replacement`);
      
      // If we stored the nonce in the database, use it
      if (donation.tx_nonce !== null && donation.tx_nonce !== undefined) {
        await replaceStuckTransaction(donation.id, txHash, wallet, provider, donation.tx_nonce);
      } else {
        // Otherwise, try to get the nonce from the transaction
        try {
          const tx = await provider.getTransaction(txHash);
          if (tx && tx.nonce !== undefined) {
            await replaceStuckTransaction(donation.id, txHash, wallet, provider, tx.nonce);
          } else {
            logger.error(`Could not retrieve nonce for transaction ${txHash}`);
          }
        } catch (txError) {
          logger.error(`Error retrieving transaction ${txHash}:`, txError);
        }
      }
    }
  } catch (error) {
    logger.error(`Error checking transaction ${txHash}:`, error);
  }
}

// Replace a stuck transaction
async function replaceStuckTransaction(donationId, oldTxHash, wallet, provider, nonce) {
  try {
    // Get current gas prices
    const feeData = await provider.getFeeData();
    
    // Calculate higher gas price (boost by 50% for replacement transactions)
    const boostFactor = 150; // 50% boost, higher than regular transactions
    
    const maxFeePerGas = (feeData.maxFeePerGas || feeData.gasPrice) * 
      BigInt(boostFactor) / BigInt(100);
    
    const maxPriorityFeePerGas = (feeData.maxPriorityFeePerGas || 
      feeData.gasPrice / 2n) * BigInt(boostFactor) / BigInt(100);
    
    // Create wallet instance
    const signer = new ethers.Wallet(wallet.private_key, provider);
    
    // Send replacement transaction (zero value to self)
    const replacementTx = await signer.sendTransaction({
      to: wallet.wallet_address,
      value: 0n,
      nonce: nonce,
      maxFeePerGas,
      maxPriorityFeePerGas,
      gasLimit: 21000n
    });
    
    logger.info(`Sent replacement transaction: ${replacementTx.hash} with nonce ${nonce}`);
    
    // Update donation record
    await db.query(
      `UPDATE direct_donations SET contract_tx_hash = $1, check_count = 0 WHERE id = $2`,
      [replacementTx.hash, donationId]
    );
  } catch (error) {
    logger.error(`Error replacing stuck transaction:`, error);
  }
}

// Check wallet balance and create donation if needed
async function checkBalanceAndCreateDonation(wallet, provider, mainContract) {
  try {
    // Get current balance
    const balance = await provider.getBalance(wallet.wallet_address);
    const balanceInMatic = Number(ethers.formatEther(balance));
    
    logger.debug(`Wallet ${wallet.wallet_address} has balance: ${balanceInMatic} MATIC`);
    
    // Skip if below minimum donation threshold
    if (balanceInMatic < CONFIG.MIN_DONATION_AMOUNT) {
      return;
    }
    
    logger.info(`Sufficient balance (${balanceInMatic} MATIC) found in wallet ${wallet.wallet_address}`);
    
    // Create a source_tx_hash placeholder since it's required (NOT NULL)
    const sourceTxHash = `balance-check-${Date.now()}`;
    
    // Create a new donation record
    const result = await db.query(`
      INSERT INTO direct_donations (
        campaign_id, wallet_address, amount, status, created_at, check_count, source_tx_hash
      ) VALUES ($1, $2, $3, $4, NOW(), 0, $5)
      RETURNING id
    `, [
      wallet.campaign_id,
      wallet.wallet_address,
      balanceInMatic.toString(),
      'pending',
      sourceTxHash
    ]);
    
    const donationId = result.rows[0].id;
    logger.info(`Created new donation record with ID: ${donationId}`);
    
    // Immediately process the donation
    await sendDonationTransaction(donationId, wallet, provider, mainContract);
  } catch (error) {
    logger.error(`Error checking balance for wallet ${wallet.wallet_address}:`, error);
  }
}

// Send donation transaction to contract
async function sendDonationTransaction(donationId, wallet, provider, mainContract) {
  try {
    // Get current wallet balance
    const balance = await provider.getBalance(wallet.wallet_address);
    
    // Skip if balance is too low
    if (balance < ethers.parseEther("0.1")) {
      logger.debug(`Skipping donation ${donationId}: insufficient balance (${ethers.formatEther(balance)} MATIC)`);
      return;
    }
    
    // Create wallet instance
    const signer = new ethers.Wallet(wallet.private_key, provider);
    
    // First, get current gas prices and estimate
    const feeData = await provider.getFeeData();
    
    // Use 20% higher gas price for faster confirmation
    const maxFeePerGas = (feeData.maxFeePerGas || feeData.gasPrice) * 
      BigInt(CONFIG.GAS_PRICE_BOOST) / BigInt(100);
    
    const maxPriorityFeePerGas = (feeData.maxPriorityFeePerGas || 
      feeData.gasPrice / 2n) * BigInt(CONFIG.GAS_PRICE_BOOST) / BigInt(100);
    
    // Estimate gas with small test amount to avoid estimation failures
    const testAmount = ethers.parseEther("0.1");
    const gasEstimate = await mainContract.connect(signer).donate.estimateGas(
      wallet.campaign_id,
      ethers.ZeroAddress, // token address (zero for native token)
      0, // token amount (0 when using native token)
      { value: testAmount }
    );
    
    // Add 30% buffer to gas limit for safety
    const gasLimit = gasEstimate * 130n / 100n;
    
    // Calculate gas cost: gasLimit * gasPrice
    const gasCost = gasLimit * (maxFeePerGas || feeData.gasPrice);
    
    // Add 50% buffer to gas cost estimation for safety
    const gasCostWithBuffer = gasCost * 150n / 100n;
    
    logger.debug(`Estimated gas cost for donation ${donationId}: ${ethers.formatEther(gasCostWithBuffer)} MATIC`);
    
    // Ensure minimum reserve
    const minReserve = ethers.parseEther("0.05");
    const finalGasReserve = gasCostWithBuffer > minReserve ? gasCostWithBuffer : minReserve;
    
    // Calculate donation amount after subtracting gas costs
    const donationAmount = balance - finalGasReserve;
    
    if (donationAmount <= 0n) {
      logger.warn(`Donation ${donationId} has insufficient funds after gas reserve`);
      await db.query(
        `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
        [donationId]
      );
      return;
    }
    
    logger.info(`Sending donation ${donationId}: ${ethers.formatEther(donationAmount)} MATIC to campaign ${wallet.campaign_id}`);
    
    // Get the nonce for this transaction
    const nonce = await signer.getNonce();
    
    // Send the transaction
    const tx = await mainContract.connect(signer).donate(
      wallet.campaign_id,
      ethers.ZeroAddress,
      0,
      {
        value: donationAmount,
        maxFeePerGas,
        maxPriorityFeePerGas,
        gasLimit,
        nonce
      }
    );
    
    logger.info(`Donation ${donationId} transaction sent: ${tx.hash} with nonce ${nonce}`);
    
    // Update donation record with transaction hash and nonce
    await db.query(
      `UPDATE direct_donations SET contract_tx_hash = $1, check_count = 0, tx_nonce = $3 WHERE id = $2`,
      [tx.hash, donationId, nonce]
    );
  } catch (error) {
    logger.error(`Error sending donation transaction for ID ${donationId}:`, error);
    
    // Mark as failed if transaction couldn't be sent
    await db.query(
      `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
      [donationId]
    );
  }
}

module.exports = { monitorDirectDonations };