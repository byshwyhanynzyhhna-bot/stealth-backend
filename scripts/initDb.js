require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

async function initDatabase() {
    console.log('🚀 Initializing Neon PostgreSQL Database...\n');
    
    const pool = new Pool({
        connectionString: process.env.NEON_DATABASE_URL,
        ssl: { rejectUnauthorized: false }
    });
    
    try {
        const schemaPath = path.join(__dirname, '..', '..', 'docs', 'schema.sql');
        const schema = fs.readFileSync(schemaPath, 'utf8');
        
        console.log('📋 Executing schema...');
        await pool.query(schema);
        
        console.log('✅ Database initialized successfully!\n');
        console.log('Created tables:');
        console.log('  - paired_users (stores device pairs)');
        console.log('  - signal_logs (audit trail)');
        console.log('\nCreated indexes:');
        console.log('  - idx_paired_users_pair_code');
        console.log('  - idx_paired_users_device_token_a');
        console.log('  - idx_paired_users_device_token_b');
        console.log('  - idx_signal_logs_pair_id');
        console.log('  - idx_signal_logs_sent_at');
        console.log('\nCreated triggers:');
        console.log('  - trigger_update_signal_time');
        
    } catch (error) {
        console.error('❌ Database initialization failed:', error.message);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

initDatabase();