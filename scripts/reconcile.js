// scripts/reconcile.js
require('dotenv').config();
const { ensureReconciliationTable, reconcileCampaigns } = require('../src/services/reconciliation');
const { initialize } = require('../src/services/blockchain');

async function main() {
  console.log('Starting campaign reconciliation');
  
  try {
    // Initialize blockchain service
    initialize();
    
    // Ensure reconciliation table exists
    await ensureReconciliationTable();
    
    // Run reconciliation
    const results = await reconcileCampaigns();
    
    console.log(`Reconciliation summary:`);
    console.log(`- Total campaigns: ${results.total}`);
    console.log(`- Updated: ${results.updated}`);
    console.log(`- Matched: ${results.matched}`);
    console.log(`- Errors: ${results.errors}`);
    
    process.exit(0);
  } catch (error) {
    console.error(`Reconciliation failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run the script
main();