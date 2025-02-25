// worker.js
require('dotenv').config();
const db = require('./src/db');
const blockchainService = require('./src/services/blockchain');

// Number of blocks to process in each batch
const BATCH_SIZE = 5000;

// How far back to go for initial indexing or jump-ahead
// 100,000 blocks is approximately 2 weeks for Ethereum
const RECENT_HISTORY_BLOCKS = 100000;

// Maximum allowed gap between current block and last indexed
// If gap is larger than this, we'll jump ahead to more recent blocks
const MAX_ACCEPTABLE_GAP = 500000; // About 3-4 weeks for most chains

// Initialize blockchain service
function initializeServices() {
  console.log('Initializing blockchain service...');
  try {
    blockchainService.initialize();
    return true;
  } catch (error) {
    console.error('Failed to initialize blockchain service:', error.message);
    console.error(error.stack);
    return false;
  }
}

async function processNetworks() {
  console.log('Starting indexing process...');
  
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
    
    for (const [network, config] of Object.entries(NETWORKS)) {
      // Skip networks without providers
      if (!providers[network]) {
        console.warn(`Provider for ${network} is not available, skipping...`);
        continue;
      }
      
      try {
        const provider = providers[network];
        const currentBlock = await provider.getBlockNumber();
        
        // Determine starting block
        let fromBlock;
        let jumpedAhead = false;
        
        if (lastIndexedBlocks[network] !== undefined) {
          // Calculate gap between current and last indexed
          const gap = currentBlock - lastIndexedBlocks[network];
          
          // If gap is too large, jump ahead to more recent blocks
          if (gap > MAX_ACCEPTABLE_GAP) {
            const oldFromBlock = lastIndexedBlocks[network] + 1;
            fromBlock = Math.max(1, currentBlock - RECENT_HISTORY_BLOCKS);
            jumpedAhead = true;
            
            console.log(`${network}: Gap too large (${gap} blocks). Jumping ahead from block ${oldFromBlock} to ${fromBlock}`);
            
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
          console.log(`${network}: First-time indexing, starting from block ${fromBlock} (${RECENT_HISTORY_BLOCKS} blocks ago)`);
        }
        
        // Safety check - don't go beyond current block
        if (fromBlock > currentBlock) {
          console.log(`${network}: No new blocks to index (${jumpedAhead ? 'after jump-ahead' : `last indexed: ${lastIndexedBlocks[network]}`}, current: ${currentBlock})`);
          continue;
        }
        
        console.log(`${network}: Current block is ${currentBlock}, ${jumpedAhead ? 'jumped ahead to' : 'last indexed block is'} ${jumpedAhead ? fromBlock : lastIndexedBlocks[network] || 'none'}`);
        
        // Calculate batch size and end block
        const blocksToProcess = Math.min(currentBlock - fromBlock + 1, BATCH_SIZE);
        const toBlock = fromBlock + blocksToProcess - 1;
        
        // Index the network
        console.log(`${network}: Indexing from block ${fromBlock} to ${toBlock} (${blocksToProcess} blocks)`);
        await indexNetwork(network, fromBlock, toBlock);
      } catch (networkError) {
        console.error(`Error processing ${network}:`, networkError.message);
        console.error(networkError.stack);
        // Continue with other networks
      }
    }
    
    console.log('Indexing process completed');
  } catch (error) {
    console.error('Indexing process failed:', error);
    throw error;
  }
}

// Run if executed directly
if (require.main === module) {
  processNetworks()
    .then(() => {
      console.log('Worker execution complete');
      process.exit(0);
    })
    .catch(error => {
      console.error('Worker execution failed:', error);
      process.exit(1);
    });
}

module.exports = { processNetworks };