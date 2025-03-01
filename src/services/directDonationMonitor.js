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

/**
 * Start monitoring campaign wallets for direct donations
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
    
    // Set up recurring intervals
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
    
    logger.info('Direct donation monitor started successfully');
    return true;
  } catch (error) {
    logger.error('Failed to start direct donation monitor:', error);
    return false;
  }
}

/**
 * Check campaign wallets for new deposits
 */
async function checkForDeposits(provider) {
  try {
    // Get all campaign wallets
    const walletsResult = await db.query('SELECT campaign_id, wallet_address FROM campaign_wallets');
    
    if (walletsResult.rows.length === 0) {
      logger.debug('No campaign wallets to monitor');
      return;
    }
    
    logger.info(`Checking ${walletsResult.rows.length} campaign wallets for new deposits`);
    
    // Check balances for each wallet
    for (const wallet of walletsResult.rows) {
      try {
        // Check the wallet's current balance
        const balance = await provider.getBalance(wallet.wallet_address);
        
        // If there's a balance (MATIC), process it as a donation
        if (balance > 0) {
          logger.info(`Found balance: ${ethers.formatEther(balance)} MATIC in wallet ${wallet.wallet_address} for campaign ${wallet.campaign_id}`);
          
          // Record the donation with the full balance amount
          await db.query(
            `INSERT INTO direct_donations (
              campaign_id, wallet_address, amount, status, source_tx_hash, created_at
            ) VALUES ($1, $2, $3, $4, $5, NOW())`,
            [
              wallet.campaign_id, 
              wallet.wallet_address, 
              ethers.formatEther(balance), 
              'pending', 
              `balance-check-${Date.now()}` // Simple placeholder
            ]
          );
        }
      } catch (error) {
        logger.error(`Error checking wallet ${wallet.wallet_address}:`, error);
      }
    }
  } catch (error) {
    logger.error('Error checking for deposits:', error);
  }
}

/**
 * Process pending direct donations by calling the contract
 */
async function processDirectDonations(provider, mainContract) {
  try {
    // Get pending donations that need processing
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
    
    // Process each pending donation
    for (const donation of pendingResult.rows) {
      try {
        // Create wallet from private key
        const wallet = new ethers.Wallet(donation.private_key, provider);
        
        // Get current wallet balance
        const balance = await provider.getBalance(donation.wallet_address);
        const amountEther = ethers.parseEther(donation.amount);

        // Estimate gas for this transaction
        const gasEstimate = await mainContract.connect(wallet).donate.estimateGas(
          donation.campaign_id,
          ethers.ZeroAddress, // Zero address for native token
          0, // Amount is sent as value
          {
            value: balance // Temporarily use full balance for estimation
          }
        );
        

        // Get gas price
        const feeData = await provider.getFeeData();
        const maxFeePerGas = feeData.maxFeePerGas || feeData.gasPrice;
        
        // Calculate actual gas cost with a 20% buffer for safety
        const gasCost = gasEstimate * maxFeePerGas * BigInt(120) / BigInt(100);

        // Calculate donation amount after gas costs
        const donationAmount = balance > gasCost ? balance - gasCost : BigInt(0);

        // Check if we have enough to make a meaningful donation
        if (donationAmount <= ethers.parseEther("0.0001")) {
          logger.warn(`Donation amount too small after gas costs for donation ${donation.id}: ${ethers.formatEther(donationAmount)}POL`);
          
          // If balance is extremely low, mark as failed
          if (donationAmount <= BigInt(0)) {
            await db.query(
              `UPDATE direct_donations SET status = 'failed', processed_at = NOW() WHERE id = $1`,
              [donation.id]
            );
          }
          
          continue;
        }

        // Proceed with the donation using calculated amount
        logger.info(`Processing donation of ${ethers.formatEther(donationAmount)}POL to campaign ${donation.campaign_id}`);

        // Call the contract with calculated donation amount
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
        
        // Update status to processing
        await db.query(
          `UPDATE direct_donations SET 
           status = 'processing', 
           contract_tx_hash = $1 
           WHERE id = $2`,
          [tx.hash, donation.id]
        );
        
        // Wait for transaction confirmation
        const receipt = await tx.wait();
        
        if (receipt.status === 1) {
          logger.info(`Donation ${donation.id} successfully processed in tx ${tx.hash}`);
          
          // Update status to completed
          await db.query(
            `UPDATE direct_donations SET 
             status = 'completed', 
             processed_at = NOW() 
             WHERE id = $1`,
            [donation.id]
          );
        } else {
          logger.error(`Donation ${donation.id} failed in tx ${tx.hash}`);
          
          // Update status to failed
          await db.query(
            `UPDATE direct_donations SET 
             status = 'failed', 
             processed_at = NOW() 
             WHERE id = $1`,
            [donation.id]
          );
        }
      } catch (error) {
        logger.error(`Error processing donation ${donation.id}:`, error);
        
        // Mark as failed after attempted processing
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

module.exports = { monitorDirectDonations };