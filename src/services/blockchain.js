// src/services/blockchain.js
const ethers = require('ethers');
const db = require('../db');
const mainChainABI = require('../config/mainChainABI.json');
const remoteChainABI = require('../config/remoteChainABI.json');
const { createLogger, format, transports } = require('winston');

// Logger configuration with production awareness
const logLevel = process.env.NODE_ENV === 'production' ? 'warn' : 'info';
const logger = createLogger({
  level: logLevel, // Only log warnings and errors in production
  format: format.combine(
    format.timestamp(),
    format.errors({ stack: true }),
    format.splat(),
    format.json()
  ),
  defaultMeta: { service: 'blockchain-indexer' },
  transports: [
    new transports.Console({
      format: format.combine(
        format.colorize(),
        format.printf(({ level, message, timestamp, service, ...meta }) => {
          // In production, only include metadata for errors
          const metaStr = (level === 'error' || process.env.NODE_ENV !== 'production') && Object.keys(meta).length ? 
            JSON.stringify(meta) : '';
          return `${timestamp} [${service}] ${level}: ${message} ${metaStr}`;
        })
      )
    }),
    // Only log to files in non-production environment or for errors
    ...(process.env.NODE_ENV !== 'production' ? [
      new transports.File({ filename: 'logs/error.log', level: 'error' }),
      new transports.File({ filename: 'logs/combined.log' })
    ] : [
      new transports.File({ filename: 'logs/error.log', level: 'error' })
    ])
  ]
});

// Helper methods for selective logging
logger.debugIf = (condition, message, meta = {}) => {
  if (condition) {
    logger.debug(message, meta);
  }
};

logger.infoIf = (condition, message, meta = {}) => {
  if (condition) {
    logger.info(message, meta);
  }
};

// Constants
const STABLE_TOKEN_DECIMALS = 8; // Can be configured based on token
const MAX_BLOCK_RANGE = 10000; // Maximum block range to process at once
const MAX_RETRY_COUNT = 3; // Maximum number of retries for RPC calls
const RETRY_DELAY_MS = 2000; // Delay between retries
const IS_DEV = process.env.NODE_ENV !== 'production';

// Performance metrics
const metrics = {
  eventsProcessed: { campaigns: 0, donations: 0, withdrawals: 0 },
  dbOperations: 0,
  errors: 0,
  processingTimeMs: 0
};

// Network configurations
const NETWORKS = {
  polygon: {
    rpc: process.env.POLYGON_RPC,
    contractAddress: process.env.POLYGON_CONTRACT_ADDRESS,
    isMain: true
  },
  ethereum: {
    rpc: process.env.ETH_RPC,
    contractAddress: process.env.ETH_CONTRACT_ADDRESS,
    isMain: false
  },
  bsc: {
    rpc: process.env.BSC_RPC,
    contractAddress: process.env.BSC_CONTRACT_ADDRESS,
    isMain: false
  },
  base: {
    rpc: process.env.BASE_RPC,
    contractAddress: process.env.BASE_CONTRACT_ADDRESS,
    isMain: false
  },
  avalanche: {
    rpc: process.env.AVALANCHE_RPC,
    contractAddress: process.env.AVALANCHE_CONTRACT_ADDRESS,
    isMain: false
  },
  optimism: {
    rpc: process.env.OPTIMISM_RPC,
    contractAddress: process.env.OPTIMISM_CONTRACT_ADDRESS,
    isMain: false
  },
  arbitrum: {
    rpc: process.env.ARBITRUM_RPC,
    contractAddress: process.env.ARBITRUM_CONTRACT_ADDRESS,
    isMain: false
  },
  sonic: {
    rpc: process.env.SONIC_RPC,
    contractAddress: process.env.SONIC_CONTRACT_ADDRESS,
    isMain: false
  },
  soneium: {
    rpc: process.env.SONEIUM_RPC,
    contractAddress: process.env.SONEIUM_CONTRACT_ADDRESS,
    isMain: false
  },
  // Add other networks...
};

// Validate environment variables
function validateEnvironment() {
  const issues = [];
  
  Object.entries(NETWORKS).forEach(([network, config]) => {
    if (!config.rpc) {
      issues.push(`Missing RPC URL for ${network}`);
    }
    if (!config.contractAddress) {
      issues.push(`Missing contract address for ${network}`);
    }
  });
  
  if (issues.length > 0) {
    logger.error(`Environment validation failed`, { issues });
    throw new Error(`Environment validation failed: ${issues.join(', ')}`);
  }
  
  logger.info('Environment validation successful');
}

// Create providers and contracts with validation
const providers = {};
const contracts = {};

function initializeProviders() {
  Object.entries(NETWORKS).forEach(([network, config]) => {
    if (!config.rpc || !config.contractAddress) {
      logger.warn(`Skipping ${network} due to missing configuration`);
      return;
    }
    
    try {
      providers[network] = new ethers.JsonRpcProvider(config.rpc);
      contracts[network] = new ethers.Contract(
        config.contractAddress,
        config.isMain ? mainChainABI : remoteChainABI,
        providers[network]
      );
      logger.info(`Initialized provider and contract for ${network}`);
    } catch (error) {
      logger.error(`Failed to initialize ${network}`, { 
        error: error.message, 
        stack: error.stack,
        network
      });
    }
  });
}

// Retry wrapper for RPC calls
async function withRetry(fn, name, ...args) {
  let lastError;
  for (let attempt = 1; attempt <= MAX_RETRY_COUNT; attempt++) {
    try {
      logger.debugIf(IS_DEV, `Attempting ${name} (try ${attempt}/${MAX_RETRY_COUNT})`);
      const result = await fn(...args);
      if (attempt > 1) {
        logger.infoIf(IS_DEV, `${name} succeeded after ${attempt} attempts`);
      }
      return result;
    } catch (error) {
      lastError = error;
      logger.warn(`${name} attempt ${attempt} failed: ${error.message}`);
      if (attempt < MAX_RETRY_COUNT) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
      }
    }
  }
  
  // If we get here, all retries failed
  logger.error(`${name} failed after ${MAX_RETRY_COUNT} attempts`);
  throw lastError;
}

// Campaign indexing
async function indexCampaignEvents(network, fromBlock, toBlock) {
  logger.infoIf(IS_DEV, `Indexing ${network} campaign events from ${fromBlock} to ${toBlock}`);
  
  if (!NETWORKS[network].isMain) {
    logger.infoIf(IS_DEV, `Skipping campaign events for non-main chain ${network}`);
    return; // Only main chain has campaign events
  }
  
  const contract = contracts[network];
  
  try {
    // Start transaction
    await db.query('BEGIN');
    metrics.dbOperations++;
    
    // Fetch created events
    const createdFilter = contract.filters.CampaignCreated();
    const createdEvents = await withRetry(
      contract.queryFilter.bind(contract), 
      'queryFilter-CampaignCreated', 
      createdFilter, 
      fromBlock, 
      toBlock
    );
    
    // Fetch edited events
    const editedFilter = contract.filters.CampaignEdited();
    const editedEvents = await withRetry(
      contract.queryFilter.bind(contract), 
      'queryFilter-CampaignEdited',
      editedFilter, 
      fromBlock, 
      toBlock
    );
    
    // Fetch ended events
    const endedFilter = contract.filters.CampaignEnded();
    const endedEvents = await withRetry(
      contract.queryFilter.bind(contract), 
      'queryFilter-CampaignEnded',
      endedFilter, 
      fromBlock, 
      toBlock
    );
    
    // Get all campaign IDs to fetch in batch
    const allCampaignIds = new Set();
    createdEvents.forEach(event => allCampaignIds.add(event.args.campaignId.toString()));
    editedEvents.forEach(event => allCampaignIds.add(event.args.campaignId.toString()));
    
    // Pre-fetch campaign data in batches to avoid multiple RPC calls
    const campaignDataMap = new Map();
    const campaignIdsList = [...allCampaignIds];
    
    // Process campaign data in batches of 20 to avoid RPC limits
    const BATCH_SIZE = 20;
    for (let i = 0; i < campaignIdsList.length; i += BATCH_SIZE) {
      const batch = campaignIdsList.slice(i, i + BATCH_SIZE);
      const campaignPromises = batch.map(id => 
        withRetry(
          contract.campaigns.bind(contract), 
          `fetch-campaign-${id}`, 
          id
        )
      );
      
      const campaignResults = await Promise.all(campaignPromises);
      batch.forEach((id, index) => {
        campaignDataMap.set(id, campaignResults[index]);
      });
    }
    
    // Prepare batch inserts for created events
    const createdValues = [];
    const createdTxValues = [];
    
    for (const event of createdEvents) {
      const campaignId = event.args.campaignId.toString();
      const creator = event.args.creator;
      
      // Get pre-fetched campaign data
      const campaign = campaignDataMap.get(campaignId);
      
      if (!campaign) {
        logger.warn(`Campaign data not found for ID ${campaignId}`);
        continue;
      }
      
      createdValues.push([
        campaignId,
        campaign.name,
        campaign.description,
        ethers.formatUnits(campaign.target, STABLE_TOKEN_DECIMALS),
        campaign.socialLink,
        campaign.imageId.toString(),
        campaign.creator,
        campaign.ended,
        ethers.formatUnits(campaign.totalStable, STABLE_TOKEN_DECIMALS),
        network,
        event.transactionHash
      ]);
      
      createdTxValues.push([
        'Campaign Created',
        creator,
        campaignId,
        network,
        event.transactionHash
      ]);
    }
    
    // Batch insert campaigns
    if (createdValues.length > 0) {
      const createdParams = [];
      const createdQueryParts = [];
      
      createdValues.forEach((values, i) => {
        const offset = i * 11; // 11 params per row
        createdQueryParts.push(`($${offset+1}, $${offset+2}, $${offset+3}, $${offset+4}, $${offset+5}, $${offset+6}, $${offset+7}, $${offset+8}, $${offset+9}, $${offset+10}, $${offset+11})`);
        createdParams.push(...values);
      });
      
      await db.query(
        `INSERT INTO campaigns (
          id, name, description, target_amount, social_link, image_id, 
          creator, ended, amount_raised, chain, tx_hash
        ) VALUES ${createdQueryParts.join(', ')}
        ON CONFLICT (id) DO NOTHING`,
        createdParams
      );
      metrics.dbOperations++;
      
      // Batch insert campaign created transactions
      const createdTxParams = [];
      const createdTxQueryParts = [];
      
      createdTxValues.forEach((values, i) => {
        const offset = i * 5; // 5 params per row
        createdTxQueryParts.push(`($${offset+1}, $${offset+2}, $${offset+3}, NOW(), $${offset+4}, $${offset+5})`);
        createdTxParams.push(...values);
      });
      
      await db.query(
        `INSERT INTO transactions (
          type, user_address, campaign_id, timestamp, chain, tx_hash
        ) VALUES ${createdTxQueryParts.join(', ')}`,
        createdTxParams
      );
      metrics.dbOperations++;
    }
    
    // Process edited events - these need individual updates
    for (const event of editedEvents) {
      const campaignId = event.args.campaignId.toString();
      
      // Get pre-fetched campaign data
      const campaign = campaignDataMap.get(campaignId);
      
      if (!campaign) {
        logger.warn(`Campaign data not found for ID ${campaignId} during edit`);
        continue;
      }
      
      await db.query(
        `UPDATE campaigns SET
          name = $1,
          description = $2,
          target_amount = $3,
          social_link = $4,
          image_id = $5,
          updated_at = NOW()
        WHERE id = $6`,
        [
          campaign.name,
          campaign.description,
          ethers.formatUnits(campaign.target, STABLE_TOKEN_DECIMALS),
          campaign.socialLink,
          campaign.imageId.toString(),
          campaignId
        ]
      );
      metrics.dbOperations++;
      
      // Record transaction
      await db.query(
        `INSERT INTO transactions (
          type, user_address, campaign_id, timestamp, chain, tx_hash
        ) VALUES ($1, $2, $3, NOW(), $4, $5)`,
        [
          'Campaign Edited',
          campaign.creator,
          campaignId,
          network,
          event.transactionHash
        ]
      );
      metrics.dbOperations++;
    }
    
    // Process ended events - batch processing where possible
    const endedValues = [];
    const endedTxValues = [];
    
    for (const event of endedEvents) {
      const campaignId = event.args.campaignId.toString();
      const finalAmount = ethers.formatUnits(event.args.finalStableValue, STABLE_TOKEN_DECIMALS);
      
      endedValues.push([
        finalAmount, 
        campaignId
      ]);
      
      endedTxValues.push([
        'Campaign Ended',
        campaignId,
        finalAmount,
        network,
        event.transactionHash
      ]);
    }
    
    // Batch update campaigns
    for (const [finalAmount, campaignId] of endedValues) {
      await db.query(
        `UPDATE campaigns SET
          ended = TRUE,
          amount_raised = $1,
          updated_at = NOW()
        WHERE id = $2`,
        [finalAmount, campaignId]
      );
      metrics.dbOperations++;
    }
    
    // Batch insert campaign ended transactions
    if (endedTxValues.length > 0) {
      const endedTxParams = [];
      const endedTxQueryParts = [];
      
      endedTxValues.forEach((values, i) => {
        const offset = i * 5; // 5 params per row
        endedTxQueryParts.push(`($${offset+1}, (SELECT creator FROM campaigns WHERE id = $${offset+2}), $${offset+2}, $${offset+3}, NOW(), $${offset+4}, $${offset+5})`);
        endedTxParams.push(...values);
      });
      
      await db.query(
        `INSERT INTO transactions (
          type, user_address, campaign_id, amount, timestamp, chain, tx_hash
        ) VALUES ${endedTxQueryParts.join(', ')}`,
        endedTxParams
      );
      metrics.dbOperations++;
    }
    
    // Commit all changes
    await db.query('COMMIT');
    metrics.dbOperations++;
    
    // Update metrics
    metrics.eventsProcessed.campaigns += createdEvents.length + editedEvents.length + endedEvents.length;
    
    // Only log results if we found events or in development mode
    const hasEvents = createdEvents.length > 0 || editedEvents.length > 0 || endedEvents.length > 0;
    logger.infoIf(IS_DEV || hasEvents, `Indexed ${createdEvents.length} created, ${editedEvents.length} edited, ${endedEvents.length} ended campaigns`);
    
  } catch (error) {
    await db.query('ROLLBACK');
    metrics.dbOperations++;
    metrics.errors++;
    
    logger.error(`Error indexing ${network} campaigns`, {
      error: error.message,
      stack: error.stack,
      fromBlock,
      toBlock,
      network
    });
    
    throw error;
  }
}

// Donation indexing
async function indexDonationEvents(network, fromBlock, toBlock) {
  // Skip non-main chains for donations
  if (!NETWORKS[network].isMain) {
    logger.infoIf(IS_DEV, `Skipping donation indexing for non-main chain ${network}`);
    return;
  }
  
  logger.infoIf(IS_DEV, `Indexing ${network} donation events from ${fromBlock} to ${toBlock}`);
  
  const contract = contracts[network];
  
  try {
    // Start transaction
    await db.query('BEGIN');
    metrics.dbOperations++;
    
    // Fetch donation events
    const donationFilter = contract.filters.DonationMade();
    const donationEvents = await withRetry(
      contract.queryFilter.bind(contract), 
      'queryFilter-DonationMade',
      donationFilter, 
      fromBlock, 
      toBlock
    );
    
    // Prepare batch values
    const donationValues = [];
    const updateCampaignValues = [];
    const transactionValues = [];
    
    for (const event of donationEvents) {
      const args = { 
        campaignId: event.args.campaignId.toString(),
        donor: event.args.donor,
        netUSDValue: event.args.netUSDValue
      };
      
      const campaignId = args.campaignId;
      const donor = args.donor;
      const amount = ethers.formatUnits(args.netUSDValue, STABLE_TOKEN_DECIMALS);
      
      donationValues.push([
        campaignId, 
        donor, 
        amount, 
        network, 
        event.transactionHash
      ]);
      
      updateCampaignValues.push([
        amount, 
        campaignId
      ]);
      
      transactionValues.push([
        'Donation', 
        donor, 
        campaignId, 
        amount, 
        network, 
        event.transactionHash
      ]);
    }
    
    // Batch insert donations
    if (donationValues.length > 0) {
      const donationParams = [];
      const donationQueryParts = [];
      
      donationValues.forEach((values, i) => {
        const offset = i * 5; // 5 params per row
        donationQueryParts.push(`($${offset+1}, $${offset+2}, $${offset+3}, NOW(), $${offset+4}, $${offset+5})`);
        donationParams.push(...values);
      });
      
      await db.query(
        `INSERT INTO donations (
          campaign_id, donor, amount, timestamp, chain, tx_hash
        ) VALUES ${donationQueryParts.join(', ')}`,
        donationParams
      );
      metrics.dbOperations++;
    }
    
    // Update campaign amounts individually to ensure consistency
    for (const [amount, campaignId] of updateCampaignValues) {
      await db.query(
        `UPDATE campaigns SET
          amount_raised = amount_raised + $1,
          updated_at = NOW()
        WHERE id = $2`,
        [amount, campaignId]
      );
      metrics.dbOperations++;
    }
    
    // Batch insert transactions
    if (transactionValues.length > 0) {
      const txParams = [];
      const txQueryParts = [];
      
      transactionValues.forEach((values, i) => {
        const offset = i * 6; // 6 params per row
        txQueryParts.push(`($${offset+1}, $${offset+2}, $${offset+3}, $${offset+4}, NOW(), $${offset+5}, $${offset+6})`);
        txParams.push(...values);
      });
      
      await db.query(
        `INSERT INTO transactions (
          type, user_address, campaign_id, amount, timestamp, chain, tx_hash
        ) VALUES ${txQueryParts.join(', ')}`,
        txParams
      );
      metrics.dbOperations++;
    }
    
    // Commit all changes
    await db.query('COMMIT');
    metrics.dbOperations++;
    
    // Update metrics
    metrics.eventsProcessed.donations += donationEvents.length;
    
    // Only log results if we found events or in development mode
    logger.infoIf(IS_DEV || donationEvents.length > 0, `Indexed ${donationEvents.length} donations`);
    
  } catch (error) {
    await db.query('ROLLBACK');
    metrics.dbOperations++;
    metrics.errors++;
    
    logger.error(`Error indexing ${network} donations`, {
      error: error.message,
      stack: error.stack,
      fromBlock,
      toBlock,
      network
    });
    
    throw error;
  }
}

// Withdrawal indexing
async function indexWithdrawalEvents(network, fromBlock, toBlock) {
  logger.infoIf(IS_DEV, `Indexing ${network} withdrawal events from ${fromBlock} to ${toBlock}`);
  
  if (!NETWORKS[network].isMain) {
    logger.infoIf(IS_DEV, `Skipping withdrawal events for non-main chain ${network}`);
    return; // Only main chain has withdrawal events
  }
  
  const contract = contracts[network];
  
  try {
    // Start transaction
    await db.query('BEGIN');
    metrics.dbOperations++;
    
    // Fetch withdrawal request events
    const requestFilter = contract.filters.WithdrawalRequested();
    const requestEvents = await withRetry(
      contract.queryFilter.bind(contract), 
      'queryFilter-WithdrawalRequested',
      requestFilter, 
      fromBlock, 
      toBlock
    );
    
    // Fetch withdrawal processed events
    const processedFilter = contract.filters.WithdrawalProcessed();
    const processedEvents = await withRetry(
      contract.queryFilter.bind(contract), 
      'queryFilter-WithdrawalProcessed',
      processedFilter, 
      fromBlock, 
      toBlock
    );
    
    // Prepare batch values for requests
    const requestValues = [];
    const requestTxValues = [];
    
    for (const event of requestEvents) {
      const requestId = event.args.requestId.toString();
      const requester = event.args.requester;
      const amount = ethers.formatUnits(event.args.amount, STABLE_TOKEN_DECIMALS);
      const token = event.args.token;
      const targetChain = event.args.targetChainId.toString();
      
      requestValues.push([
        requestId,
        requester,
        amount,
        token,
        targetChain,
        'Requested',
        network,
        event.transactionHash
      ]);
      
      requestTxValues.push([
        'Withdrawal Requested',
        requester,
        amount,
        token,
        targetChain,
        network,
        event.transactionHash
      ]);
    }
    
    // Batch insert withdrawal requests
    if (requestValues.length > 0) {
      const requestParams = [];
      const requestQueryParts = [];
      
      requestValues.forEach((values, i) => {
        const offset = i * 8; // 8 params per row
        requestQueryParts.push(`($${offset+1}, $${offset+2}, $${offset+3}, $${offset+4}, $${offset+5}, $${offset+6}, NOW(), $${offset+7}, $${offset+8})`);
        requestParams.push(...values);
      });
      
      await db.query(
        `INSERT INTO withdrawals (
          id, user_address, amount, token, target_chain, status, 
          request_timestamp, chain, tx_hash
        ) VALUES ${requestQueryParts.join(', ')}
        ON CONFLICT (id) DO NOTHING`,
        requestParams
      );
      metrics.dbOperations++;
    }
    
    // Batch insert withdrawal request transactions
    if (requestTxValues.length > 0) {
      const requestTxParams = [];
      const requestTxQueryParts = [];
      
      requestTxValues.forEach((values, i) => {
        const offset = i * 7; // 7 params per row
        requestTxQueryParts.push(`($${offset+1}, $${offset+2}, $${offset+3}, $${offset+4}, $${offset+5}, NOW(), $${offset+6}, $${offset+7})`);
        requestTxParams.push(...values);
      });
      
      await db.query(
        `INSERT INTO transactions (
          type, user_address, amount, token, target_chain, timestamp, chain, tx_hash
        ) VALUES ${requestTxQueryParts.join(', ')}`,
        requestTxParams
      );
      metrics.dbOperations++;
    }
    
    // Process processed events - need to be done individually due to dependencies
    for (const event of processedEvents) {
      const requestId = event.args.requestId.toString();
      
      // Get withdrawal data for transaction log
      const withdrawal = await db.query(
        'SELECT * FROM withdrawals WHERE id = $1',
        [requestId]
      );
      metrics.dbOperations++;
      
      if (withdrawal.rows.length > 0) {
        const withdrawalData = withdrawal.rows[0];
        
        await db.query(
          `UPDATE withdrawals SET
            status = $1,
            processed_timestamp = NOW(),
            processed_tx_hash = $2
          WHERE id = $3`,
          ['Processed', event.transactionHash, requestId]
        );
        metrics.dbOperations++;
        
        // Record transaction
        await db.query(
          `INSERT INTO transactions (
            type, user_address, amount, token, target_chain, timestamp, chain, tx_hash
          ) VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7)`,
          [
            'Withdrawal Processed',
            withdrawalData.user_address,
            withdrawalData.amount,
            withdrawalData.token,
            withdrawalData.target_chain,
            network,
            event.transactionHash
          ]
        );
        metrics.dbOperations++;
      }
    }
    
    // Commit all changes
    await db.query('COMMIT');
    metrics.dbOperations++;
    
    // Update metrics
    metrics.eventsProcessed.withdrawals += requestEvents.length + processedEvents.length;
    
    // Only log results if we found events or in development mode
    const hasEvents = requestEvents.length > 0 || processedEvents.length > 0;
    logger.infoIf(IS_DEV || hasEvents, `Indexed ${requestEvents.length} withdrawal requests, ${processedEvents.length} processed withdrawals`);
    
  } catch (error) {
    await db.query('ROLLBACK');
    metrics.dbOperations++;
    metrics.errors++;
    
    logger.error(`Error indexing ${network} withdrawals`, {
      error: error.message,
      stack: error.stack,
      fromBlock,
      toBlock,
      network
    });
    
    throw error;
  }
}

// Process a chunk of blocks
async function indexNetworkChunk(network, fromBlock, toBlock) {
  logger.infoIf(IS_DEV, `Processing chunk for ${network} from block ${fromBlock} to ${toBlock}`);
  
  try {
    // Start timer for performance metrics
    const startTime = Date.now();
    
    // Index campaign events (only for main chain)
    if (NETWORKS[network].isMain) {
      await indexCampaignEvents(network, fromBlock, toBlock);
    }
    
    // Index donation events (only for main chain)
    if (NETWORKS[network].isMain) {
      await indexDonationEvents(network, fromBlock, toBlock);
    }
    
    // Index withdrawal events (only for main chain)
    if (NETWORKS[network].isMain) {
      await indexWithdrawalEvents(network, fromBlock, toBlock);
    }
    
    // Calculate processing time
    const processingTime = Date.now() - startTime;
    metrics.processingTimeMs += processingTime;
    
    // Only log completion details if in dev mode
    logger.infoIf(IS_DEV, `Completed chunk processing for ${network}`, {
      fromBlock,
      toBlock,
      processingTimeMs: processingTime
    });
    
    return toBlock;
  } catch (error) {
    logger.error(`Failed to process chunk for ${network}`, {
      error: error.message,
      stack: error.stack,
      fromBlock,
      toBlock
    });
    throw error;
  }
}

// Main indexing function with block range chunking
async function indexNetwork(network, fromBlock, toBlock) {
  logger.infoIf(IS_DEV, `Starting indexing for ${network} from block ${fromBlock} to ${toBlock}`);
  
  try {
    // Reset metrics for this run
    metrics.eventsProcessed = { campaigns: 0, donations: 0, withdrawals: 0 };
    metrics.dbOperations = 0;
    metrics.errors = 0;
    metrics.processingTimeMs = 0;
    
    // Process in chunks if range is too large
    if (toBlock - fromBlock > MAX_BLOCK_RANGE) {
      logger.infoIf(IS_DEV, `Large block range detected, breaking into chunks of ${MAX_BLOCK_RANGE} blocks`);
      
      const chunks = [];
      for (let i = fromBlock; i < toBlock; i += MAX_BLOCK_RANGE) {
        chunks.push([i, Math.min(i + MAX_BLOCK_RANGE - 1, toBlock)]);
      }
      
      logger.infoIf(IS_DEV, `Created ${chunks.length} chunks for processing`);
      
      let lastProcessedBlock = fromBlock;
      for (const [chunkFrom, chunkTo] of chunks) {
        lastProcessedBlock = await indexNetworkChunk(network, chunkFrom, chunkTo);
      }
      
      // Update last indexed block
      await db.query(
        `INSERT INTO indexer_state (chain, last_indexed_block, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (chain) DO UPDATE SET
           last_indexed_block = $2,
           updated_at = NOW()`,
        [network, lastProcessedBlock]
      );
      
      // Only log completion details if in dev mode or we found events
      const foundEvents = 
        metrics.eventsProcessed.campaigns > 0 || 
        metrics.eventsProcessed.donations > 0 || 
        metrics.eventsProcessed.withdrawals > 0;
      
      logger.infoIf(IS_DEV || foundEvents, `Completed chunked indexing for ${network}`, { metrics });
      
      return lastProcessedBlock;
    } else {
      // For smaller ranges, process directly
      const processedBlock = await indexNetworkChunk(network, fromBlock, toBlock);
      
      // Update last indexed block
      await db.query(
        `INSERT INTO indexer_state (chain, last_indexed_block, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (chain) DO UPDATE SET
           last_indexed_block = $2,
           updated_at = NOW()`,
        [network, processedBlock]
      );
      
      // Only log completion details if in dev mode or we found events
      const foundEvents = 
        metrics.eventsProcessed.campaigns > 0 || 
        metrics.eventsProcessed.donations > 0 || 
        metrics.eventsProcessed.withdrawals > 0;
      
      logger.infoIf(IS_DEV || foundEvents, `Completed indexing for ${network}`, { metrics });
      
      return processedBlock;
    }
  } catch (error) {
    logger.error(`Failed to index ${network}`, {
      error: error.message,
      stack: error.stack,
      fromBlock,
      toBlock,
      metrics
    });
    throw error;
  }
}

// Get current indexer metrics
function getMetrics() {
  return {
    ...metrics,
    networks: Object.keys(NETWORKS).length,
    activeNetworks: Object.keys(providers).length
  };
}

// Initialize the module
function initialize() {
  validateEnvironment();
  initializeProviders();
  logger.info('Blockchain indexer initialized successfully');
}

// Export the module
module.exports = {
  initialize,
  indexNetwork,
  getMetrics,
  providers,
  contracts,
  NETWORKS
};