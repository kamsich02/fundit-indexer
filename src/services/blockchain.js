// src/services/blockchain.js
const ethers = require('ethers')
const db = require('../db');
const mainChainABI = require('../config/mainChainABI.json');
const remoteChainABI = require('../config/remoteChainABI.json');

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

// Create providers and contracts
const providers = {};
const contracts = {};

Object.entries(NETWORKS).forEach(([network, config]) => {
  providers[network] = new ethers.JsonRpcProvider({url: config.rpc});
  contracts[network] = new ethers.Contract(
    config.contractAddress,
    config.isMain ? mainChainABI : remoteChainABI,
    providers[network]
  );
});

// Campaign indexing
async function indexCampaignEvents(network, fromBlock, toBlock) {
  console.log(`Indexing ${network} campaign events from ${fromBlock} to ${toBlock}`);
  
  if (!NETWORKS[network].isMain) {
    console.log(`Skipping campaign events for non-main chain ${network}`);
    return; // Only main chain has campaign events
  }
  
  const contract = contracts[network];
  const client = await db.query('BEGIN');
  
  try {
    // Fetch created events
    const createdFilter = contract.filters.CampaignCreated();
    const createdEvents = await contract.queryFilter(createdFilter, fromBlock, toBlock);
    
    // Fetch edited events
    const editedFilter = contract.filters.CampaignEdited();
    const editedEvents = await contract.queryFilter(editedFilter, fromBlock, toBlock);
    
    // Fetch ended events
    const endedFilter = contract.filters.CampaignEnded();
    const endedEvents = await contract.queryFilter(endedFilter, fromBlock, toBlock);
    
    // Process created events
    for (const event of createdEvents) {
      const campaignId = event.args.campaignId.toString();
      const creator = event.args.creator;
      
      // Fetch campaign details from contract
      const campaign = await contract.campaigns(campaignId);
      
      await client.query(
        `INSERT INTO campaigns (
          id, name, description, target_amount, social_link, image_id, 
          creator, ended, amount_raised, chain, tx_hash
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (id) DO NOTHING`,
        [
          campaignId,
          campaign.name,
          campaign.description,
          ethers.formatUnits(campaign.target, 8),
          campaign.socialLink,
          campaign.imageId.toString(),
          campaign.creator,
          campaign.ended,
          ethers.formatUnits(campaign.totalStable, 8),
          network,
          event.transactionHash
        ]
      );
      
      // Record transaction
      await client.query(
        `INSERT INTO transactions (
          type, user_address, campaign_id, timestamp, chain, tx_hash
        ) VALUES ($1, $2, $3, NOW(), $4, $5)`,
        [
          'Campaign Created',
          creator,
          campaignId,
          network,
          event.transactionHash
        ]
      );
    }
    
    // Process edited events
    for (const event of editedEvents) {
      const campaignId = event.args.campaignId.toString();
      
      // Fetch updated campaign details
      const campaign = await contract.campaigns(campaignId);
      
      await client.query(
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
          ethers.formatUnits(campaign.target, 8),
          campaign.socialLink,
          campaign.imageId.toString(),
          campaignId
        ]
      );
      
      // Record transaction
      await client.query(
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
    }
    
    // Process ended events
    for (const event of endedEvents) {
      const campaignId = event.args.campaignId.toString();
      const finalAmount = ethers.formatUnits(event.args.finalStableValue, 8);
      
      await client.query(
        `UPDATE campaigns SET
          ended = TRUE,
          amount_raised = $1,
          updated_at = NOW()
        WHERE id = $2`,
        [finalAmount, campaignId]
      );
      
      // Record transaction
      await client.query(
        `INSERT INTO transactions (
          type, user_address, campaign_id, amount, timestamp, chain, tx_hash
        ) VALUES ($1, (SELECT creator FROM campaigns WHERE id = $2), $2, $3, NOW(), $4, $5)`,
        [
          'Campaign Ended',
          campaignId,
          finalAmount,
          network,
          event.transactionHash
        ]
      );
    }
    
    // Commit all changes
    await client.query('COMMIT');
    console.log(`Indexed ${createdEvents.length} created, ${editedEvents.length} edited, ${endedEvents.length} ended campaigns`);
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error(`Error indexing ${network} campaigns:`, error);
    throw error;
  }
}

// Donation indexing
async function indexDonationEvents(network, fromBlock, toBlock) {
  console.log(`Indexing ${network} donation events from ${fromBlock} to ${toBlock}`);
  
  const contract = contracts[network];
  const client = await db.query('BEGIN');
  
  try {
    // Fetch donation events
    const donationFilter = contract.filters.DonationMade();
    const donationEvents = await contract.queryFilter(donationFilter, fromBlock, toBlock);
    
    for (const event of donationEvents) {
      const args = NETWORKS[network].isMain 
        ? { 
            campaignId: event.args.campaignId.toString(),
            donor: event.args.donor,
            netUSDValue: event.args.netUSDValue
          }
        : { 
            donationId: event.args.donationId.toString(),
            campaignId: event.args.campaignId.toString(),
            donor: event.args.donor,
            netUSDValue: event.args.netUSDValue
          };
      
      const campaignId = args.campaignId;
      const donor = args.donor;
      const amount = ethers.formatUnits(args.netUSDValue, 8);
      
      // Record donation
      await client.query(
        `INSERT INTO donations (
          campaign_id, donor, amount, timestamp, chain, tx_hash
        ) VALUES ($1, $2, $3, NOW(), $4, $5)`,
        [campaignId, donor, amount, network, event.transactionHash]
      );
      
      // Update campaign amount raised
      await client.query(
        `UPDATE campaigns SET
          amount_raised = amount_raised + $1,
          updated_at = NOW()
        WHERE id = $2`,
        [amount, campaignId]
      );
      
      // Record transaction
      await client.query(
        `INSERT INTO transactions (
          type, user_address, campaign_id, amount, timestamp, chain, tx_hash
        ) VALUES ($1, $2, $3, $4, NOW(), $5, $6)`,
        [
          'Donation',
          donor,
          campaignId,
          amount,
          network,
          event.transactionHash
        ]
      );
    }
    
    // Commit all changes
    await client.query('COMMIT');
    console.log(`Indexed ${donationEvents.length} donations`);
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error(`Error indexing ${network} donations:`, error);
    throw error;
  }
}

// Withdrawal indexing
async function indexWithdrawalEvents(network, fromBlock, toBlock) {
  console.log(`Indexing ${network} withdrawal events from ${fromBlock} to ${toBlock}`);
  
  if (!NETWORKS[network].isMain) {
    console.log(`Skipping withdrawal events for non-main chain ${network}`);
    return; // Only main chain has withdrawal events
  }
  
  const contract = contracts[network];
  const client = await db.query('BEGIN');
  
  try {
    // Fetch withdrawal request events
    const requestFilter = contract.filters.WithdrawalRequested();
    const requestEvents = await contract.queryFilter(requestFilter, fromBlock, toBlock);
    
    // Fetch withdrawal processed events
    const processedFilter = contract.filters.WithdrawalProcessed();
    const processedEvents = await contract.queryFilter(processedFilter, fromBlock, toBlock);
    
    // Process request events
    for (const event of requestEvents) {
      const requestId = event.args.requestId.toString();
      const requester = event.args.requester;
      const amount = ethers.formatUnits(event.args.amount, 8);
      const token = event.args.token;
      const targetChain = event.args.targetChainId.toString();
      
      await client.query(
        `INSERT INTO withdrawals (
          id, user_address, amount, token, target_chain, status, 
          request_timestamp, chain, tx_hash
        ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8)
        ON CONFLICT (id) DO NOTHING`,
        [
          requestId,
          requester,
          amount,
          token,
          targetChain,
          'Requested',
          network,
          event.transactionHash
        ]
      );
      
      // Record transaction
      await client.query(
        `INSERT INTO transactions (
          type, user_address, amount, token, target_chain, timestamp, chain, tx_hash
        ) VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7)`,
        [
          'Withdrawal Requested',
          requester,
          amount,
          token,
          targetChain,
          network,
          event.transactionHash
        ]
      );
    }
    
    // Process processed events
    for (const event of processedEvents) {
      const requestId = event.args.requestId.toString();
      
      // Get withdrawal data for transaction log
      const withdrawal = await client.query(
        'SELECT * FROM withdrawals WHERE id = $1',
        [requestId]
      );
      
      if (withdrawal.rows.length > 0) {
        const withdrawalData = withdrawal.rows[0];
        
        await client.query(
          `UPDATE withdrawals SET
            status = $1,
            processed_timestamp = NOW(),
            processed_tx_hash = $2
          WHERE id = $3`,
          ['Processed', event.transactionHash, requestId]
        );
        
        // Record transaction
        await client.query(
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
      }
    }
    
    // Commit all changes
    await client.query('COMMIT');
    console.log(`Indexed ${requestEvents.length} withdrawal requests, ${processedEvents.length} processed withdrawals`);
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error(`Error indexing ${network} withdrawals:`, error);
    throw error;
  }
}

// Main indexing function
async function indexNetwork(network, fromBlock, toBlock) {
  console.log(`Starting indexing for ${network} from block ${fromBlock} to ${toBlock}`);
  
  try {
    // Index campaign events (only for main chain)
    if (NETWORKS[network].isMain) {
      await indexCampaignEvents(network, fromBlock, toBlock);
    }
    
    // Index donation events (for all chains)
    await indexDonationEvents(network, fromBlock, toBlock);
    
    // Index withdrawal events (only for main chain)
    if (NETWORKS[network].isMain) {
      await indexWithdrawalEvents(network, fromBlock, toBlock);
    }
    
    // Update last indexed block
    await db.query(
      `INSERT INTO indexer_state (chain, last_indexed_block, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (chain) DO UPDATE SET
         last_indexed_block = $2,
         updated_at = NOW()`,
      [network, toBlock]
    );
    
    console.log(`Completed indexing for ${network}`);
    
    return toBlock;
  } catch (error) {
    console.error(`Failed to index ${network}:`, error);
    throw error;
  }
}

module.exports = {
  indexNetwork,
  providers,
  contracts,
  NETWORKS
};