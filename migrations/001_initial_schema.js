// migrations/001_initial_schema.js
const db = require('../src/db');

async function up() {
  // Create campaigns table
  await db.query(`
    CREATE TABLE campaigns (
      id VARCHAR(255) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      description TEXT,
      target_amount DECIMAL(24, 8) NOT NULL,
      social_link TEXT,
      image_id VARCHAR(255),
      creator VARCHAR(255) NOT NULL,
      ended BOOLEAN DEFAULT FALSE,
      amount_raised DECIMAL(24, 8) DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      chain VARCHAR(50) NOT NULL,
      tx_hash VARCHAR(255)
    );
  `);

  // Create donations table
  await db.query(`
    CREATE TABLE donations (
      id SERIAL PRIMARY KEY,
      campaign_id VARCHAR(255) REFERENCES campaigns(id),
      donor VARCHAR(255) NOT NULL,
      amount DECIMAL(24, 8) NOT NULL,
      timestamp TIMESTAMP DEFAULT NOW(),
      chain VARCHAR(50) NOT NULL,
      tx_hash VARCHAR(255) NOT NULL
    );
  `);

  // Create transactions table
  await db.query(`
    CREATE TABLE transactions (
      id SERIAL PRIMARY KEY,
      type VARCHAR(50) NOT NULL,
      user_address VARCHAR(255) NOT NULL,
      campaign_id VARCHAR(255),
      amount DECIMAL(24, 8),
      token VARCHAR(255),
      target_chain VARCHAR(50),
      timestamp TIMESTAMP DEFAULT NOW(),
      chain VARCHAR(50) NOT NULL,
      tx_hash VARCHAR(255) NOT NULL
    );
  `);

  // Create withdrawals table
  await db.query(`
    CREATE TABLE withdrawals (
      id VARCHAR(255) PRIMARY KEY,
      user_address VARCHAR(255) NOT NULL,
      amount DECIMAL(24, 8) NOT NULL,
      token VARCHAR(255) NOT NULL,
      target_chain VARCHAR(50) NOT NULL,
      status VARCHAR(50) DEFAULT 'Requested',
      request_timestamp TIMESTAMP DEFAULT NOW(),
      processed_timestamp TIMESTAMP,
      chain VARCHAR(50) NOT NULL,
      tx_hash VARCHAR(255) NOT NULL,
      processed_tx_hash VARCHAR(255)
    );
  `);

  // Create indexer_state table
  await db.query(`
    CREATE TABLE indexer_state (
      chain VARCHAR(50) PRIMARY KEY,
      last_indexed_block BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  console.log('Migration completed successfully');
}

async function down() {
  await db.query('DROP TABLE IF EXISTS withdrawals');
  await db.query('DROP TABLE IF EXISTS transactions');
  await db.query('DROP TABLE IF EXISTS donations');
  await db.query('DROP TABLE IF EXISTS campaigns');
  await db.query('DROP TABLE IF EXISTS indexer_state');
  
  console.log('Rollback completed successfully');
}

module.exports = { up, down };