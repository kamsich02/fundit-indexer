// worker.js
require('dotenv').config();
const db = require('./src/db');
const { indexNetwork, providers, NETWORKS } = require('./src/services/blockchain');

// Number of blocks to process in each batch
const BATCH_SIZE = 5000;

async function processNetworks() {
  console.log('Starting indexing process...');
  
  try {
    // Get last indexed blocks
    const result = await db.query('SELECT chain, last_indexed_block FROM indexer_state');
    const lastIndexedBlocks = {};
    
    result.rows.forEach(row => {
      lastIndexedBlocks[row.chain] = parseInt(row.last_indexed_block);
    });
    
    // Process each network
    for (const [network, config] of Object.entries(NETWORKS)) {
      const provider = providers[network];
      const currentBlock = await provider.getBlockNumber();
      const lastIndexed = lastIndexedBlocks[network] || 0;
      
      console.log(`${network}: Current block is ${currentBlock}, last indexed block is ${lastIndexed}`);
      
      if (currentBlock <= lastIndexed) {
        console.log(`${network}: No new blocks to index`);
        continue;
      }
      
      // Calculate batch size
      const blocksToProcess = Math.min(currentBlock - lastIndexed, BATCH_SIZE);
      const fromBlock = lastIndexed + 1;
      const toBlock = fromBlock + blocksToProcess - 1;
      
      // Index the network
      console.log(`${network}: Indexing from block ${fromBlock} to ${toBlock}`);
      await indexNetwork(network, fromBlock, toBlock);
    }
    
    console.log('Indexing process completed');
  } catch (error) {
    console.error('Indexing process failed:', error);
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