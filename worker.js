// worker.js
require('dotenv').config();
const db = require('./src/db');
const blockchainService = require('./src/services/blockchain');

// Block processing configuration
const CATCHUP_BATCH_SIZE = 5000;   // Larger batch size when catching up
const REALTIME_BATCH_SIZE = 100;   // Smaller batch size for frequent updates
const RECENT_HISTORY_BLOCKS = 100000;  // How far back to jump if needed
const MAX_ACCEPTABLE_GAP = 500000; // Gap threshold for jump-ahead
const REALTIME_THRESHOLD = 200;    // Consider caught up if within this many blocks

// Logging configuration - change to false for production
const VERBOSE_LOGGING = process.env.NODE_ENV !== 'production';
const LOG_STATS_ONLY = !VERBOSE_LOGGING;
const ALWAYS_LOG_ERRORS = true;

// Logging helper
function log(message, type = 'info', force = false) {
  if (force || VERBOSE_LOGGING || (type === 'error' && ALWAYS_LOG_ERRORS)) {
    if (type === 'error') {
      console.error(`[${new Date().toISOString()}] ERROR: ${message}`);
    } else {
      console.log(`[${new Date().toISOString()}] ${message}`);
    }
  }
}

// Stats tracking to log only important info
let totalEventsProcessed = 0;
let totalBlocksProcessed = 0;
let totalNetworksProcessed = 0;
let networksInRealtimeMode = 0;
let startTime;

// Initialize blockchain service
function initializeServices() {
  log('Initializing blockchain service...');
  try {
    blockchainService.initialize();
    return true;
  } catch (error) {
    log(`Failed to initialize blockchain service: ${error.message}`, 'error', true);
    if (VERBOSE_LOGGING) {
      log(error.stack, 'error');
    }
    return false;
  }
}
async function processNetworks() {
  startTime = Date.now();
  log('Starting indexing process...', 'info', LOG_STATS_ONLY);
  
  // Reset stats
  totalEventsProcessed = 0;
  totalBlocksProcessed = 0;
  totalNetworksProcessed = 0;
  networksInRealtimeMode = 0;
  
  try {
    // Initialize services first
    const initialized = initializeServices();
    if (!initialized) {
      throw new Error('Failed to initialize services');
    }
    
    // Get last indexed blocks
    const result = await db.query('SELECT chain, last_indexed_block FROM indexer_state');
    const lastIndexedBlocks = {};
    
    result.rows.forEach(row => {
      lastIndexedBlocks[row.chain] = parseInt(row.last_indexed_block);
    });
    
    // Process each network
    const { NETWORKS, providers, indexNetwork } = blockchainService;
    const networkCount = Object.keys(NETWORKS).length;
    let networksProcessed = 0;
    
    for (const [network, config] of Object.entries(NETWORKS)) {
      // Skip networks without providers
      if (!providers[network]) {
        log(`Provider for ${network} is not available, skipping...`, 'info', VERBOSE_LOGGING);
        continue;
      }
      
      // For non-main chains, skip completely since we only care about main chain events
      if (!config.isMain) {
        log(`${network} is not the main chain, skipping completely`, 'info', VERBOSE_LOGGING);
        networksProcessed++;
        continue;
      }
      
      try {
        const provider = providers[network];
        const currentBlock = await provider.getBlockNumber();
        
        // Determine starting block
        let fromBlock;
        let jumpedAhead = false;
        let realtimeMode = false;
        
        if (lastIndexedBlocks[network] !== undefined) {
          // Calculate gap between current and last indexed
          const gap = currentBlock - lastIndexedBlocks[network];
          
          // If gap is very small, we're in realtime mode
          if (gap <= REALTIME_THRESHOLD) {
            realtimeMode = true;
            networksInRealtimeMode++;
          }
          
          // If gap is too large, jump ahead to more recent blocks
          if (gap > MAX_ACCEPTABLE_GAP) {
            const oldFromBlock = lastIndexedBlocks[network] + 1;
            fromBlock = Math.max(1, currentBlock - RECENT_HISTORY_BLOCKS);
            jumpedAhead = true;
            
            log(`${network}: Gap too large (${gap} blocks). Jumping ahead from block ${oldFromBlock} to ${fromBlock}`, 'info', true);
            
            // Update the last indexed block in database to reflect our jump
            await db.query(
              'UPDATE indexer_state SET last_indexed_block = $1, updated_at = NOW() WHERE chain = $2',
              [fromBlock - 1, network]
            );
          } else {
            // Normal case - continue from the next block
            fromBlock = lastIndexedBlocks[network] + 1;
          }
        } else {
          // First time indexing this chain - start from recent history
          fromBlock = Math.max(1, currentBlock - RECENT_HISTORY_BLOCKS);
          log(`${network}: First-time indexing, starting from block ${fromBlock}`, 'info', true);
        }
        
        // Safety check - don't go beyond current block
        if (fromBlock > currentBlock) {
          log(`${network}: No new blocks to index`, 'info', VERBOSE_LOGGING);
          networksProcessed++;
          continue;
        }
        
        // Choose appropriate batch size based on how close we are to the current block
        const batchSize = realtimeMode ? REALTIME_BATCH_SIZE : CATCHUP_BATCH_SIZE;
        
        log(`${network}: ${realtimeMode ? 'REALTIME' : 'CATCHUP'} mode, last indexed: ${lastIndexedBlocks[network] || 'none'}, current: ${currentBlock}`, 'info', VERBOSE_LOGGING);
        
        // Calculate batch size and end block
        const blocksToProcess = Math.min(currentBlock - fromBlock + 1, batchSize);
        const toBlock = fromBlock + blocksToProcess - 1;
        
        // Only log the range if verbose or processing significant blocks
        if (VERBOSE_LOGGING || blocksToProcess > 10) {
          log(`${network}: Indexing from block ${fromBlock} to ${toBlock} (${blocksToProcess} blocks)`, 'info', VERBOSE_LOGGING);
        }
        
        // Index the network
        const metrics = await indexNetwork(network, fromBlock, toBlock);
        
        // Update stats
        totalBlocksProcessed += blocksToProcess;
        if (metrics && metrics.eventsProcessed) {
          const networkEvents = 
            (metrics.eventsProcessed.campaigns || 0) + 
            (metrics.eventsProcessed.donations || 0) + 
            (metrics.eventsProcessed.withdrawals || 0);
          totalEventsProcessed += networkEvents;
          
          // Log only if events were found (important info)
          if (networkEvents > 0) {
            log(`${network}: Processed ${networkEvents} events in ${blocksToProcess} blocks`, 'info', true);
          }
        }
        
        networksProcessed++;
        totalNetworksProcessed++;
      } catch (networkError) {
        log(`Error processing ${network}: ${networkError.message}`, 'error', true);
        if (VERBOSE_LOGGING) {
          log(networkError.stack, 'error');
        }
      }
    }
    
    // Log a summary of what was done - always log this
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    log(`Indexing completed: Processed ${totalBlocksProcessed} blocks across ${totalNetworksProcessed} networks in ${duration}s (${networksInRealtimeMode} in realtime mode, ${totalEventsProcessed} events found)`, 'info', true);
    
  } catch (error) {
    log(`Indexing process failed: ${error.message}`, 'error', true);
    if (VERBOSE_LOGGING) {
      log(error.stack, 'error');
    }
    throw error;
  }
}

// Get chain-specific stats for frontend display
async function getIndexerStatus() {
  try {
    const status = {};
    const { NETWORKS, providers } = blockchainService;
    
    // Get last indexed blocks
    const result = await db.query('SELECT chain, last_indexed_block, updated_at FROM indexer_state');
    const lastIndexedData = {};
    
    result.rows.forEach(row => {
      lastIndexedData[row.chain] = {
        lastBlock: parseInt(row.last_indexed_block),
        lastUpdated: row.updated_at
      };
    });
    
    // Get current block for each chain
    for (const [network, config] of Object.entries(NETWORKS)) {
      if (!providers[network]) continue;
      
      try {
        const currentBlock = await providers[network].getBlockNumber();
        const lastIndexed = lastIndexedData[network] || { lastBlock: 0, lastUpdated: null };
        const blocksRemaining = currentBlock - lastIndexed.lastBlock;
        
        status[network] = {
          currentBlock,
          lastIndexedBlock: lastIndexed.lastBlock,
          blocksRemaining,
          lastUpdated: lastIndexed.lastUpdated,
          syncStatus: lastIndexed.lastBlock > 0 ? 
            ((lastIndexed.lastBlock / currentBlock) * 100).toFixed(2) + '%' : '0%',
          isRealtime: blocksRemaining <= REALTIME_THRESHOLD
        };
      } catch (error) {
        log(`Error getting status for ${network}: ${error.message}`, 'error', true);
        status[network] = { error: error.message };
      }
    }
    
    return status;
  } catch (error) {
    log(`Error getting indexer status: ${error.message}`, 'error', true);
    return { error: error.message };
  }
}

// Run if executed directly
if (require.main === module) {
  processNetworks()
    .then(() => {
      log('Worker execution complete', 'info', LOG_STATS_ONLY);
      process.exit(0);
    })
    .catch(error => {
      log(`Worker execution failed: ${error.message}`, 'error', true);
      process.exit(1);
    });
}

module.exports = { 
  processNetworks,
  getIndexerStatus
};