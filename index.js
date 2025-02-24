// index.js
require('dotenv').config();
const app = require('./src/api');
const { runMigrations } = require('./src/db/migrations');

const PORT = process.env.PORT || 3000;

async function start() {
  try {
    // Run migrations first
    console.log('Running database migrations...');
    await runMigrations();
    console.log('Migrations completed successfully');
    
    // Then start the server
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error('Failed to start:', error);
    process.exit(1);
  }
}

// Start the application
start();