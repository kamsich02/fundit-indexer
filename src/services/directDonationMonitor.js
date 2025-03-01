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
    format.splat(),
    format.json()
  ),
  defaultMeta: { service: 'direct-donation-monitor' },
  transports: [
    new transports.Console({
      format: format.combine(
        format.colorize(),
        format.printf(({ level, message, timestamp, service, ...meta }) => {
          const metaStr = Object.keys(meta).length ? JSON.stringify(meta) : '';
          return `${timestamp} [${service}] ${level}: ${message} ${metaStr}`;
        })
      )
    }),
    new transports.File({ filename: 'logs/direct-donations.log' })
  ]
});

// Constants
const POLL_INTERVAL = 60000; // Check every minute
const MIN_BLOCK_CONFIRMATIONS = 5; // Wait for 5 block confirmations
const GAS_PERCENTAGE = 20; // Reserve 20% of funds for gas
const STABLE_TOKEN_DECIMALS = 8;
const BALANCE_THRESHOLD_MATIC = 1; // Only process wallets with at least 1 MATIC
const TX_TIMEOUT = 120000; // 2 minutes timeout for transaction confirmation

/**
 * Start monitoring campaign wallets for direct donations.
 */
async function monitorDirectDonations() {
  logger.info('Starting direct donation monitor');

  try {
    // Get the main chain provider and contract (Polygon)
    const mainChain = Object.keys(NETWORKS).find(network => NETWORKS[network].isMain);
    if (!mainChain || !providers[mainChain] || !contracts[mainChain]) {
      throw new Error('Main chain provider or contract not available');
    }
    const provider = providers[mainChain];
    const mainContract = contracts[mainChain];

    logger.info(`Using ${mainChain} as the main chain for donations`);

    // Initial check for pending deposits
    await checkForDeposits(provider);
    // Process any pending donations
    await processDirectDonations(provider, mainContract);
    // Check for processing donations that need status updates
    await updateProcessingDonations(provider);

    // Set up recurring intervals:
    setInterval(async () => {
      try {
        await checkForDeposits(provider);
      } catch (error) {
        logger.error('Error checking for deposits:', error);
      }
    }, POLL_INTERVAL);

    setInterval(async () => {
      try {
        await processDirectDonations(provider, mainContract);
      } catch (error) {
        logger.error('Error processing donations:', error);
      }
    }, POLL_INTERVAL * 2); // Process less frequently than checking

    // Add interval for updating processing donations status
    setInterval(async () => {
      try {
        await updateProcessingDonations(provider);
      } catch (error) {
        logger.error('Error updating processing donations:', error);
      }
    }, POLL_INTERVAL); 

    logger.info('Direct donation monitor started successfully');
    return true;
  } catch (error) {
    logger.error('Failed to start direct donation monitor:', error);
    return false;
  }
}

/**
 * Check campaign wallets for new deposits.
 * For each wallet, skip if:
 *   - Its balance is below the threshold (e.g. 1 MATIC)
 *   - There is already a pending or processing donation in the database.
 * Otherwise, record the donation as "pending".
 */
async function checkForDeposits(provider) {
  try {
    const walletsResult = await db.query('SELECT campaign_id, wallet_address FROM campaign_wallets');
    if (walletsResult.rows.length === 0) {
      logger.debug('No campaign wallets to monitor');
      return;
    }

    logger.info(`Checking ${walletsResult.rows.length} campaign wallets for new deposits`);

    for (const wallet of walletsResult.rows) {
      try {
        // Check if a donation for this wallet is already pending or processing
        const pendingResult = await db.query(
          `SELECT id FROM direct_donations 
           WHERE wallet_address = $1 AND status IN ('pending', 'processing')
           LIMIT 1`,
          [wallet.wallet_address]
        );
        if (pendingResult.rows.length > 0) {
          logger.debug(`Skipping wallet ${wallet.wallet_address} as it already has a pending donation.`);
          continue;
        }

        // Get the wallet's current balance
        const balance = await provider.getBalance(wallet.wallet_address);
        const balanceEther = Number(ethers.formatEther(balance));
        if (balanceEther < BALANCE_THRESHOLD_MATIC) {
          logger.debug(`Skipping wallet ${wallet.wallet_address} for campaign ${wallet.campaign_id}: balance (${balanceEther} MATIC) is below threshold.`);
          continue;
        }

        logger.info(`Found balance: ${ethers.formatEther(balance)} MATIC in wallet ${wallet.wallet_address} for campaign ${wallet.campaign_id}`);

        // Record the donation with status "pending"
        await db.query(
          `INSERT INTO direct_donations (
            campaign_id, wallet_address, amount, status, source_tx_hash, created_at
          ) VALUES ($1, $2, $3, $4, $5, NOW())`,
          [
            wallet.campaign_id,
            wallet.wallet_address,
            ethers.formatEther(balance),
            'pending',
            `balance-check-${Date.now()}`
          ]
        );
      } catch (error) {
        logger.error(`Error checking wallet ${wallet.wallet_address}:`, error);
      }
    }
  } catch (error) {
    logger.error('Error checking for deposits:', error);
  }
}

/**
 * Process pending direct donations by calling the donation contract.
 * For each pending donation, re-check the wallet balance, estimate the gas cost,
 * and if the remaining balance (after subtracting gas) is sufficient, send the donation.
 * Then update the donation status in the database based on the transaction result.
 */
async function processDirectDonations(provider, mainContract) {
  try {
    const pendingResult = await db.query(
      `SELECT d.id, d.campaign_id, d.wallet_address, d.amount, d.source_tx_hash, w.private_key
       FROM direct_donations d
       JOIN campaign_wallets w ON d.wallet_address = w.wallet_address
       WHERE d.status = 'pending'
       ORDER BY d.created_at ASC`
    );

    if (pendingResult.rows.length === 0) {
      logger.debug('No pending donations to process');
      return;
    }

    logger.info(`Processing ${pendingResult.rows.length} pending donations`);

    for (const donation of pendingResult.rows) {
      try {
        // Double-check that the donation is still in pending status
        const statusCheck = await db.query(
          'SELECT status FROM direct_donations WHERE id = $1',
          [donation.id]
        );
        
        if (statusCheck.rows.length === 0 || statusCheck.rows[0].status !== 'pending') {
          logger.info(`Donation ${donation.id} is no longer in pending status, skipping`);
          continue;
        }
        
        // Create a wallet instance using the stored private key
        const wallet = new ethers.Wallet(donation.private_key, provider);
        // Re-check the current balance
        const balance = await provider.getBalance(donation.wallet_address);

        // Estimate gas for this donation transaction (using the full balance for estimation)
        const gasEstimate = await mainContract.connect(wallet).donate.estimateGas(
          donation.campaign_id,
          ethers.ZeroAddress, // For native token donation
          0,
          { value: balance }
        );

        // Retrieve fee data and determine max fee per gas
        const feeData = await provider.getFeeData();
        const maxFeePerGas = feeData.maxFeePerGas || feeData.gasPrice;

        // Calculate total gas cost with a 20% buffer for safety
        const gasCost = gasEstimate * maxFeePerGas * BigInt(120) / BigInt(100);
        // Calculate the donation amount: balance minus gas cost
        const donationAmount = balance > gasCost ? balance - gasCost : BigInt(0);

        // Skip donation if amount is too low (less than 0.0001 MATIC)
        if (donationAmount <= ethers.parseEther("0.0001")) {
          logger.warn(`Donation amount too small for donation ${donation.id}: ${ethers.formatEther(donationAmount)} POL`);
          if (donationAmount <= BigInt(0)) {
            await db.query(
              `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          }
          continue;
        }

        logger.info(`Processing donation of ${ethers.formatEther(donationAmount)} POL for campaign ${donation.campaign_id}`);

        // Send the donation transaction
        const tx = await mainContract.connect(wallet).donate(
          donation.campaign_id,
          ethers.ZeroAddress,
          0,
          {
            value: donationAmount,
            maxFeePerGas: maxFeePerGas,
            maxPriorityFeePerGas: feeData.maxPriorityFeePerGas || feeData.gasPrice,
            gasLimit: gasEstimate * BigInt(120) / BigInt(100)
          }
        );

        logger.info(`Donation transaction sent: ${tx.hash}`);

        // Update the donation record to "processing" with the tx hash
        await db.query(
          `UPDATE direct_donations SET status = 'processing', contract_tx_hash = $1 WHERE id = $2`,
          [tx.hash, donation.id]
        );

        // Wait for transaction confirmation with timeout
        try {
          const receipt = await Promise.race([
            tx.wait(),
            new Promise((_, reject) => 
              setTimeout(() => reject(new Error('Transaction confirmation timeout')), TX_TIMEOUT)
            )
          ]);
          
          if (receipt.status === 1) {
            logger.info(`Donation ${donation.id} successfully processed in tx ${tx.hash}`);
            await db.query(
              `UPDATE direct_donations SET status = 'completed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          } else {
            logger.error(`Donation ${donation.id} failed in tx ${tx.hash}`);
            await db.query(
              `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          }
        } catch (timeoutError) {
          // If timeout occurs, we leave the status as "processing" and will check it in updateProcessingDonations
          logger.warn(`Timeout waiting for confirmation of tx ${tx.hash} for donation ${donation.id}`);
        }
      } catch (error) {
        logger.error(`Error processing donation ${donation.id}:`, error);
        // Mark donation as failed if any error occurs during processing
        await db.query(
          `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
          [donation.id]
        );
      }
    }
  } catch (error) {
    logger.error('Error processing direct donations:', error);
  }
}

/**
 * Check the status of donations marked as "processing" and update them.
 * This handles cases where a transaction was sent but our process didn't see the confirmation,
 * or where we hit a timeout waiting for confirmation.
 */
async function updateProcessingDonations(provider) {
  try {
    const processingResult = await db.query(
      `SELECT id, contract_tx_hash 
       FROM direct_donations 
       WHERE status = 'processing' AND contract_tx_hash IS NOT NULL
       ORDER BY created_at ASC`
    );
    
    if (processingResult.rows.length === 0) {
      return;
    }
    
    logger.info(`Checking status of ${processingResult.rows.length} processing donations`);
    
    for (const donation of processingResult.rows) {
      try {
        // Skip if no transaction hash
        if (!donation.contract_tx_hash) continue;
        
        // Get transaction receipt to check status
        const receipt = await provider.getTransactionReceipt(donation.contract_tx_hash);
        
        // If receipt exists, update status based on success or failure
        if (receipt) {
          if (receipt.status === 1) {
            logger.info(`Donation ${donation.id} confirmed successful from tx ${donation.contract_tx_hash}`);
            await db.query(
              `UPDATE direct_donations SET status = 'completed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          } else {
            logger.error(`Donation ${donation.id} confirmed failed from tx ${donation.contract_tx_hash}`);
            await db.query(
              `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          }
        } else {
          // No receipt yet - transaction still pending
          // We could add logic here to time out very old transactions
          const donationAge = await db.query(
            `SELECT EXTRACT(EPOCH FROM (NOW() - created_at)) as age_seconds FROM direct_donations WHERE id = $1`,
            [donation.id]
          );
          
          // If processing for over 10 minutes with no receipt, mark as failed
          if (donationAge.rows[0] && donationAge.rows[0].age_seconds > 600) {
            logger.warn(`Donation ${donation.id} processing timed out after 10 minutes`);
            await db.query(
              `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          }
        }
      } catch (error) {
        logger.error(`Error checking status of donation ${donation.id}:`, error);
      }
    }
  } catch (error) {
    logger.error('Error updating processing donations:', error);
  }
}

module.exports = { monitorDirectDonations };