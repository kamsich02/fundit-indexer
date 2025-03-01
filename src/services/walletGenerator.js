// src/services/walletGenerator.js
const ethers = require('ethers');
const crypto = require('crypto');

/**
 * Generate a deterministic wallet for a campaign
 * @param {string} campaignId - The campaign ID
 * @param {string} masterSeed - Master seed for deterministic generation
 * @returns {Object} - Wallet information
 */

function generateCampaignWallet(campaignId, masterSeed) {
  if (!masterSeed) {
    throw new Error('Master seed is required for wallet generation');
  }
  
  // Create a unique seed for this campaign using HMAC-SHA256
  const campaignSeed = crypto
    .createHmac('sha256', masterSeed)
    .update(`campaign-${campaignId}`)
    .digest();
  
  // Create a wallet with the private key
  const privateKey = '0x' + Buffer.from(campaignSeed).toString('hex');
  const wallet = new ethers.Wallet(privateKey);
  
  return {
    address: wallet.address,
    privateKey: wallet.privateKey,
    campaignId
  };
}

module.exports = { generateCampaignWallet };