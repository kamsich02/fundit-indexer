// migrations/runner.js
const fs = require('fs');
const path = require('path');

async function runMigrations() {
  console.log('Running migrations...');
  
  // Get all migration files
  const files = fs.readdirSync(__dirname)
    .filter(file => file !== 'runner.js' && file.endsWith('.js'))
    .sort();
  
  for (const file of files) {
    console.log(`Running migration: ${file}`);
    const migration = require(path.join(__dirname, file));
    await migration.up();
  }
  
  console.log('All migrations completed');
}

runMigrations()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('Migration failed:', err);
    process.exit(1);
  });