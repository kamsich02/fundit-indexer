// src/api/index.js
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const db = require('../db');
const { getIndexerStatus } = require('../worker');

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

// Get all campaigns with pagination
app.get('/api/campaigns', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    const page = parseInt(req.query.page) || 1;
    const offset = (page - 1) * limit;
    const ended = req.query.ended === 'true';
    
    // Get total count
    const countResult = await db.query(
      'SELECT COUNT(*) FROM campaigns WHERE ended = $1',
      [ended]
    );
    const total = parseInt(countResult.rows[0].count);
    
    // Get campaigns
    const result = await db.query(
      `SELECT * FROM campaigns 
       WHERE ended = $1
       ORDER BY created_at DESC
       LIMIT $2 OFFSET $3`,
      [ended, limit, offset]
    );
    
    // Format response
    const campaigns = result.rows.map(row => ({
      id: row.id,
      title: row.name,
      description: row.description,
      image: `/assets/images/campaign-${row.image_id}.png`,
      amountRaised: parseFloat(row.amount_raised),
      targetAmount: parseFloat(row.target_amount),
      createdAt: row.created_at,
      status: row.ended ? 'Ended' : 'Ongoing',
      creator: row.creator,
      moreinfo: row.social_link
    }));
    
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
    
    const row = result.rows[0];
    
    // Format campaign
    const campaign = {
      id: row.id,
      title: row.name,
      description: row.description,
      image: `/assets/images/campaign-${row.image_id}.png`,
      amountRaised: parseFloat(row.amount_raised),
      targetAmount: parseFloat(row.target_amount),
      createdAt: row.created_at,
      status: row.ended ? 'Ended' : 'Ongoing',
      creator: row.creator,
      moreinfo: row.social_link
    };
    
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
    const campaigns = result.rows.map(row => ({
      id: row.id,
      title: row.name,
      description: row.description,
      image: `/assets/images/campaign-${row.image_id}.png`,
      amountRaised: parseFloat(row.amount_raised),
      targetAmount: parseFloat(row.target_amount),
      createdAt: row.created_at,
      status: row.ended ? 'Ended' : 'Ongoing',
      creator: row.creator,
      moreinfo: row.social_link
    }));
    
    res.json(campaigns);
  } catch (error) {
    console.error('Error getting user campaigns:', error);
    res.status(500).json({ error: 'Failed to fetch user campaigns' });
  }
});

module.exports = app;