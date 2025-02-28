// migrations/002_reconciliation_table.js
const db = require('../src/db');

async function up() {
  // Create reconciliation_log table
  await db.query(`
    CREATE TABLE IF NOT EXISTS reconciliation_log (
      id SERIAL PRIMARY KEY,
      campaign_id VARCHAR(255) REFERENCES campaigns(id),
      previous_value DECIMAL(24, 8) NOT NULL,
      new_value DECIMAL(24, 8) NOT NULL,
      discrepancy DECIMAL(24, 8) NOT NULL,
      reconciled_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Add last_reconciled column to campaigns table
  await db.query(`
    ALTER TABLE campaigns 
    ADD COLUMN IF NOT EXISTS last_reconciled TIMESTAMP;
  `);

  console.log('Reconciliation table migration completed');
}

async function down() {
  await db.query('DROP TABLE IF EXISTS reconciliation_log');
  await db.query('ALTER TABLE campaigns DROP COLUMN IF EXISTS last_reconciled');
  
  console.log('Reconciliation table rollback completed');
}

module.exports = { up, down };