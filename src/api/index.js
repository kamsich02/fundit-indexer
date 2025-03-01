// src/api/index.js
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const db = require('../db');
const { getIndexerStatus } = require('../../worker');
const { generateCampaignWallet } = require('../services/walletGenerator');

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100
});

app.use(limiter);

// Get indexer status (for real-time frontend updates)
app.get('/api/indexer-status', async (req, res) => {
  try {
    const status = await getIndexerStatus();
    res.json(status);
  } catch (error) {
    console.error('Error getting indexer status:', error);
    res.status(500).json({ error: 'Failed to fetch indexer status' });
  }
});

// NEW: Campaign search endpoint
app.get('/api/campaigns/search', async (req, res) => {
  try {
    const query = req.query.query || '';
    const limit = parseInt(req.query.limit) || 20;
    const page = parseInt(req.query.page) || 1;
    const offset = (page - 1) * limit;
    const ended = req.query.ended === 'true';
    
    // Check if the query is a pure numeric ID
    const isPureNumeric = /^\d+$/.test(query.trim());
    
    let sqlQuery, queryParams;
    
    if (isPureNumeric) {
      // Prioritize exact ID match first
      const exactMatch = await db.query(
        'SELECT * FROM campaigns WHERE id = $1',
        [query.trim()]
      );
      
      // If we found an exact ID match, return just that campaign
      if (exactMatch.rows.length > 0) {
        const campaigns = exactMatch.rows.map(formatCampaign);
        
        return res.json({
          campaigns,
          pagination: {
            total: 1,
            page: 1,
            limit,
            totalPages: 1
          }
        });
      }
    }
    
    // If no exact ID match or query is not numeric, perform text search
    sqlQuery = `
      SELECT * FROM campaigns 
      WHERE ended = $1 AND (
        CAST(id AS TEXT) ILIKE $2 OR
        name ILIKE $2 OR 
        description ILIKE $2
      )
      ORDER BY created_at DESC
      LIMIT $3 OFFSET $4
    `;
    
    queryParams = [ended, `%${query}%`, limit, offset];
    
    // Execute the search query
    const result = await db.query(sqlQuery, queryParams);
    
    // Get total count for pagination
    const countSql = `
      SELECT COUNT(*) FROM campaigns 
      WHERE ended = $1 AND (
        CAST(id AS TEXT) ILIKE $2 OR
        name ILIKE $2 OR 
        description ILIKE $2
      )
    `;
    
    const countResult = await db.query(countSql, [ended, `%${query}%`]);
    const total = parseInt(countResult.rows[0].count);
    
    // Format campaigns
    const campaigns = result.rows.map(formatCampaign);
    
    res.json({
      campaigns,
      pagination: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit)
      }
    });
  } catch (error) {
    console.error('Error searching campaigns:', error);
    res.status(500).json({ error: 'Failed to search campaigns' });
  }
});

// Get all campaigns with pagination
app.get('/api/campaigns', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    const page = parseInt(req.query.page) || 1;
    const offset = (page - 1) * limit;
    const ended = req.query.ended === 'true';
    const sort = req.query.sort || 'newest'; // Support sort parameter
    
    // Get total count
    const countResult = await db.query(
      'SELECT COUNT(*) FROM campaigns WHERE ended = $1',
      [ended]
    );
    const total = parseInt(countResult.rows[0].count);
    
    // Determine sort order
    const sortOrder = sort === 'newest' ? 'DESC' : 'ASC';
    
    // Get campaigns
    const result = await db.query(
      `SELECT * FROM campaigns 
       WHERE ended = $1
       ORDER BY created_at ${sortOrder}
       LIMIT $2 OFFSET $3`,
      [ended, limit, offset]
    );
    
    // Format response
    const campaigns = result.rows.map(formatCampaign);
    
    res.json({
      campaigns,
      pagination: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit)
      }
    });
  } catch (error) {
    console.error('Error getting campaigns:', error);
    res.status(500).json({ error: 'Failed to fetch campaigns' });
  }
});

// Get single campaign by ID
app.get('/api/campaigns/:id', async (req, res) => {
  try {
    const result = await db.query(
      'SELECT * FROM campaigns WHERE id = $1',
      [req.params.id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    
    const campaign = formatCampaign(result.rows[0]);
    
    res.json(campaign);
  } catch (error) {
    console.error('Error getting campaign:', error);
    res.status(500).json({ error: 'Failed to fetch campaign' });
  }
});

// Get user transaction history
app.get('/api/transactions/:address', async (req, res) => {
  try {
    const address = req.params.address;
    const limit = parseInt(req.query.limit) || 20;
    const page = parseInt(req.query.page) || 1;
    const offset = (page - 1) * limit;
    
    const result = await db.query(
      `SELECT * FROM transactions 
       WHERE user_address = $1
       ORDER BY timestamp DESC
       LIMIT $2 OFFSET $3`,
      [address, limit, offset]
    );
    
    // Format transactions
    const transactions = result.rows.map(row => ({
      id: row.id,
      type: row.type,
      amount: row.amount ? parseFloat(row.amount) : null,
      token: row.token || 'USD',
      chain: row.chain,
      date: row.timestamp,
      status: 'Completed',
      txhash: row.tx_hash
    }));
    
    res.json(transactions);
  } catch (error) {
    console.error('Error getting transactions:', error);
    res.status(500).json({ error: 'Failed to fetch transactions' });
  }
});

// Get campaigns by creator
app.get('/api/user-campaigns/:address', async (req, res) => {
  try {
    const address = req.params.address;
    
    const result = await db.query(
      `SELECT * FROM campaigns 
       WHERE creator = $1
       ORDER BY created_at DESC`,
      [address]
    );
    
    // Format campaigns
    const campaigns = result.rows.map(formatCampaign);
    
    res.json(campaigns);
  } catch (error) {
    console.error('Error getting user campaigns:', error);
    res.status(500).json({ error: 'Failed to fetch user campaigns' });
  }
});

// Helper function to format campaign data
function formatCampaign(row) {
  return {
    id: row.id,
    title: row.name,
    description: row.description,
    image: row.image_id,
    amountRaised: parseFloat(row.amount_raised),
    targetAmount: parseFloat(row.target_amount),
    createdAt: row.created_at,
    status: row.ended ? 'Ended' : 'Ongoing',
    creator: row.creator,
    moreinfo: row.social_link
  };
}

// Get direct donation wallet for a campaign
app.get('/api/campaigns/:id/direct-wallet', async (req, res) => {
  try {
    // Check if campaign exists
    const campaignResult = await db.query(
      'SELECT * FROM campaigns WHERE id = $1',
      [req.params.id]
    );
    
    if (campaignResult.rows.length === 0) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    
    // Check if wallet already exists for this campaign
    const walletResult = await db.query(
      'SELECT wallet_address FROM campaign_wallets WHERE campaign_id = $1',
      [req.params.id]
    );
    
    if (walletResult.rows.length > 0) {
      // Return existing wallet
      return res.json({
        campaign_id: req.params.id,
        wallet_address: walletResult.rows[0].wallet_address,
        network: 'polygon',
        token: 'POL'
      });
    }
    
    // Generate a new wallet for this campaign
    const masterSeed = process.env.WALLET_MASTER_SEED;
    if (!masterSeed) {
      return res.status(500).json({ error: 'Wallet generation not configured' });
    }
    
    const wallet = generateCampaignWallet(req.params.id, masterSeed);
    
    // Save to database (including private key)
    await db.query(
      'INSERT INTO campaign_wallets (campaign_id, wallet_address, private_key) VALUES ($1, $2, $3)',
      [req.params.id, wallet.address, wallet.privateKey]
    );
    
    // Return wallet (without private key)
    res.json({
      campaign_id: req.params.id,
      wallet_address: wallet.address,
      network: 'polygon',
      token: 'MATIC'
    });
  } catch (error) {
    console.error('Error generating campaign wallet:', error);
    res.status(500).json({ error: 'Failed to generate wallet' });
  }
});

// Get direct donations for a campaign
app.get('/api/campaigns/:id/direct-donations', async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, amount, status, source_tx_hash, contract_tx_hash, created_at, processed_at
       FROM direct_donations
       WHERE campaign_id = $1
       ORDER BY created_at DESC`,
      [req.params.id]
    );
    
    res.json(result.rows);
  } catch (error) {
    console.error('Error getting direct donations:', error);
    res.status(500).json({ error: 'Failed to fetch direct donations' });
  }
});

module.exports = app;