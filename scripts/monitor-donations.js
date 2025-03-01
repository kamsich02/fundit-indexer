// scripts/monitor-donations.js
require('dotenv').config();
const { monitorDirectDonations } = require('../src/services/directDonationMonitor');
const { initialize } = require('../src/services/blockchain');

async function main() {
  console.log('Starting direct donation monitor');
  
  try {
    // Initialize blockchain service
    initialize();
    
    // Start monitoring for direct donations
    const success = await monitorDirectDonations();
    
    if (success) {
      console.log('Direct donation monitor running. Press Ctrl+C to exit.');
      
      // Keep the process running
      process.on('SIGINT', () => {
        console.log('Direct donation monitor stopping...');
        process.exit(0);
      });
    } else {
      console.error('Failed to start direct donation monitor');
      process.exit(1);
    }
  } catch (error) {
    console.error('Error starting direct donation monitor:', error);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run the script
main();