// src/db/migrations.js
const db = require('./index');

async function ensureMigrationsTable() {
  // Create the migrations table if it doesn't exist
  await db.query(`
    CREATE TABLE IF NOT EXISTS migrations (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL UNIQUE,
      executed_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

async function checkMigrationStatus(migrationName) {
  const result = await db.query(
    'SELECT * FROM migrations WHERE name = $1',
    [migrationName]
  );
  return result.rows.length > 0;
}

async function markMigrationExecuted(migrationName) {
  await db.query(
    'INSERT INTO migrations (name) VALUES ($1)',
    [migrationName]
  );
}

async function runMigrations() {
  await ensureMigrationsTable();
  
  const migrations = [
    { name: '001_initial_schema', up: require('../../migrations/001_initial_schema').up },
    { name: '002_reconciliation_table', up: require('../../migrations/002_reconciliation_table.js').up },
    { name: '003_campaign_wallets', up: require('../../migrations/003_campaign_wallets.js').up },
    { name: '004_direct_donations', up: require('../../migrations/004_direct_donations.js').up }
  ];
  
  for (const migration of migrations) {
    const isExecuted = await checkMigrationStatus(migration.name);
    
    if (!isExecuted) {
      console.log(`Running migration: ${migration.name}`);
      await migration.up();
      await markMigrationExecuted(migration.name);
      console.log(`Migration ${migration.name} completed`);
    } else {
      console.log(`Migration ${migration.name} already executed, skipping`);
    }
  }
}

module.exports = { runMigrations };