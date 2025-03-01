// migrations/004_direct_donations.js
const db = require('../src/db');

async function up() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS direct_donations (
      id SERIAL PRIMARY KEY,
      campaign_id VARCHAR(255) NOT NULL REFERENCES campaigns(id),
      wallet_address VARCHAR(42) NOT NULL,
      amount DECIMAL(24, 8) NOT NULL,
      status VARCHAR(20) DEFAULT 'pending' NOT NULL,
      source_tx_hash VARCHAR(66) NOT NULL,
      contract_tx_hash VARCHAR(66),
      created_at TIMESTAMP DEFAULT NOW(),
      processed_at TIMESTAMP
    );
  `);

  console.log('Direct donations table migration completed');
}

async function down() {
  await db.query('DROP TABLE IF EXISTS direct_donations');
  console.log('Direct donations table rollback completed');
}

module.exports = { up, down };