// src/services/reconciliation.js
const ethers = require('ethers');
const db = require('../db');
const { providers, contracts, NETWORKS } = require('./blockchain');
const { createLogger, format, transports } = require('winston');

// Constants
const STABLE_TOKEN_DECIMALS = 8;
const RECONCILIATION_THRESHOLD = 0.01; // 0.01 USD threshold for discrepancies
const BATCH_SIZE = 50; // Number of campaigns to process in one batch

// Logger configuration
const logger = createLogger({
  level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
  format: format.combine(
    format.timestamp(),
    format.errors({ stack: true }),
    format.splat(),
    format.json()
  ),
  defaultMeta: { service: 'reconciliation-service' },
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
    new transports.File({ filename: 'logs/reconciliation.log' })
  ]
});

/**
 * Reconcile campaign data between database and blockchain
 */
async function reconcileCampaigns() {
  logger.info('Starting campaign reconciliation process');
  
  try {
    // Get main chain (Polygon) provider and contract
    const mainNetwork = Object.keys(NETWORKS).find(key => NETWORKS[key].isMain);
    if (!mainNetwork || !providers[mainNetwork] || !contracts[mainNetwork]) {
      throw new Error('Main network provider or contract not available');
    }
    
    const provider = providers[mainNetwork];
    const contract = contracts[mainNetwork];
    
    // Get all campaigns from the database
    const dbCampaigns = await db.query(
      'SELECT id, name, amount_raised, ended FROM campaigns'
    );
    
    logger.info(`Found ${dbCampaigns.rows.length} campaigns in database`);
    
    // Process campaigns in batches to avoid overwhelming the RPC
    const campaignBatches = [];
    for (let i = 0; i < dbCampaigns.rows.length; i += BATCH_SIZE) {
      campaignBatches.push(dbCampaigns.rows.slice(i, i + BATCH_SIZE));
    }
    
    let updatedCount = 0;
    let errorCount = 0;
    let matchCount = 0;
    
    for (const [batchIndex, batch] of campaignBatches.entries()) {
      logger.info(`Processing batch ${batchIndex + 1}/${campaignBatches.length} (${batch.length} campaigns)`);
      
      // Process each campaign in the batch
      const batchPromises = batch.map(async (dbCampaign) => {
        try {
          // Get campaign data from blockchain
          const campaignId = dbCampaign.id;
          const chainCampaign = await contract.campaigns(campaignId);
          
          // Format amounts for comparison
          const chainAmountRaised = parseFloat(ethers.formatUnits(chainCampaign.totalStable, STABLE_TOKEN_DECIMALS));
          const dbAmountRaised = parseFloat(dbCampaign.amount_raised);
          
          // Calculate discrepancy
          const discrepancy = Math.abs(chainAmountRaised - dbAmountRaised);
          
          // Check if reconciliation is needed
          if (discrepancy > RECONCILIATION_THRESHOLD) {
            logger.info(`Discrepancy found for campaign ${campaignId}: DB=${dbAmountRaised}, Chain=${chainAmountRaised}, Diff=${discrepancy}`);
            
            // Update the database with the blockchain value
            await db.query(
              `UPDATE campaigns SET 
                amount_raised = $1,
                ended = $2,
                updated_at = NOW(),
                last_reconciled = NOW()
              WHERE id = $3`,
              [chainAmountRaised, chainCampaign.ended, campaignId]
            );
            
            // Log the reconciliation
            await db.query(
              `INSERT INTO reconciliation_log (
                campaign_id, previous_value, new_value, discrepancy, reconciled_at
              ) VALUES ($1, $2, $3, $4, NOW())`,
              [campaignId, dbAmountRaised, chainAmountRaised, discrepancy]
            );
            
            updatedCount++;
            return {
              campaignId,
              status: 'updated',
              discrepancy,
              dbValue: dbAmountRaised,
              chainValue: chainAmountRaised
            };
          } else {
            matchCount++;
            return {
              campaignId,
              status: 'match',
              discrepancy
            };
          }
        } catch (error) {
          errorCount++;
          logger.error(`Error reconciling campaign ${dbCampaign.id}: ${error.message}`);
          return {
            campaignId: dbCampaign.id,
            status: 'error',
            error: error.message
          };
        }
      });
      
      // Wait for all campaigns in the batch to be processed
      await Promise.all(batchPromises);
      
      // Small delay between batches to avoid rate limiting
      if (batchIndex < campaignBatches.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    logger.info(`Reconciliation completed: ${updatedCount} updated, ${matchCount} matched, ${errorCount} errors`);
    
    return {
      total: dbCampaigns.rows.length,
      updated: updatedCount,
      matched: matchCount,
      errors: errorCount
    };
    
  } catch (error) {
    logger.error(`Reconciliation process failed: ${error.message}`, { stack: error.stack });
    throw error;
  }
}

/**
 * Create the reconciliation_log table if it doesn't exist
 */
async function ensureReconciliationTable() {
  try {
    // Check if the table exists
    const tableExists = await db.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'reconciliation_log'
      );
    `);
    
    if (!tableExists.rows[0].exists) {
      logger.info('Creating reconciliation_log table');
      
      // Create the table
      await db.query(`
        CREATE TABLE reconciliation_log (
          id SERIAL PRIMARY KEY,
          campaign_id VARCHAR(255) REFERENCES campaigns(id),
          previous_value DECIMAL(24, 8) NOT NULL,
          new_value DECIMAL(24, 8) NOT NULL,
          discrepancy DECIMAL(24, 8) NOT NULL,
          reconciled_at TIMESTAMP DEFAULT NOW()
        );
      `);
      
      // Add last_reconciled column to campaigns table if it doesn't exist
      await db.query(`
        ALTER TABLE campaigns 
        ADD COLUMN IF NOT EXISTS last_reconciled TIMESTAMP;
      `);
      
      logger.info('Reconciliation table created successfully');
    }
    
    return true;
  } catch (error) {
    logger.error(`Failed to create reconciliation table: ${error.message}`);
    return false;
  }
}

module.exports = {
  reconcileCampaigns,
  ensureReconciliationTable
};