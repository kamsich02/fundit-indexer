// migrations/003_campaign_wallets.js
const db = require('../src/db');

async function up() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS campaign_wallets (
      id SERIAL PRIMARY KEY,
      campaign_id VARCHAR(255) NOT NULL REFERENCES campaigns(id),
      wallet_address VARCHAR(42) NOT NULL UNIQUE,
      private_key VARCHAR(66) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW(),
      CONSTRAINT unique_campaign_wallet UNIQUE (campaign_id, wallet_address)
    );
    
    CREATE INDEX idx_campaign_wallets_address ON campaign_wallets(wallet_address);
  `);

  console.log('Campaign wallets table migration completed');
}

async function down() {
  await db.query('DROP TABLE IF EXISTS campaign_wallets');
  console.log('Campaign wallets table rollback completed');
}

module.exports = { up, down };