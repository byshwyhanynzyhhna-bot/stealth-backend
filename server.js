require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000,
    pingInterval: 25000
});

const pool = new Pool({
    connectionString: process.env.NEON_DATABASE_URL,
    max: 10,
    ssl: { rejectUnauthorized: false }
});

const connectedDevices = new Map();
const webClients = new Map();

async function query(sql, params) {
    const client = await pool.connect();
    try {
        return await client.query(sql, params);
    } finally {
        client.release();
    }
}

async function findPair(pairCode) {
    const result = await query('SELECT * FROM paired_users WHERE pair_code = $1 AND is_active = TRUE', [pairCode]);
    return result.rows[0];
}

async function createPair(pairCode, deviceToken, deviceName) {
    const existing = await findPair(pairCode);
    
    if (existing) {
        if (existing.device_token_a === deviceToken || existing.device_token_b === deviceToken) {
            return { success: true, pair: existing, isNew: false };
        }
        
        if (!existing.device_token_b || existing.device_token_b === '') {
            const result = await query('UPDATE paired_users SET device_token_b = $1, device_name_b = $2, updated_at = NOW() WHERE id = $3 RETURNING *', [deviceToken, deviceName, existing.id]);
            return { success: true, pair: result.rows[0], isNew: false };
        }
        
        return { success: false, error: 'Pair code already has two devices' };
    }
    
    const result = await query('INSERT INTO paired_users (pair_code, device_token_a, device_name_a) VALUES ($1, $2, $3) RETURNING *', [pairCode, deviceToken, deviceName]);
    return { success: true, pair: result.rows[0], isNew: true };
}

async function triggerSignal(pairCode, senderToken, alertMode) {
    const pair = await findPair(pairCode);
    if (!pair) return { success: false, error: 'Pair not found' };
    
    const signalId = uuidv4();
    const signalType = alertMode === 'continuous' ? 'alarm' : 'nudge';
    
    await query('INSERT INTO signal_logs (pair_id, signal_type, alert_mode, sender_token) VALUES ($1, $2, $3, $4)', [pair.id, signalType, alertMode, senderToken]);
    
    const recipientToken = pair.device_token_a === senderToken ? pair.device_token_b : pair.device_token_a;
    
    if (recipientToken) {
        const recipientSocket = connectedDevices.get(recipientToken);
        if (recipientSocket) {
            recipientSocket.emit('signal-received', { type: signalType, alertMode: alertMode, signalId: signalId, pairCode: pairCode });
        }
        
        const webClient = webClients.get(recipientToken);
        if (webClient) {
            webClient.emit('signal-received', { type: signalType, alertMode: alertMode, signalId: signalId, pairCode: pairCode });
        }
    }
    
    const senderSocket = connectedDevices.get(senderToken);
    if (senderSocket) {
        senderSocket.emit('signal-sent', { success: true, signalId: signalId });
    }
    
    const webSender = webClients.get(senderToken);
    if (webSender) {
        webSender.emit('signal-sent', { success: true, signalId: signalId });
    }
    
    console.log('Signal ' + signalType + ' sent (' + alertMode + ') from pair ' + pairCode);
    
    return { success: true, signalId: signalId };
}

async function killAlarm(pairCode, requesterToken) {
    const pair = await findPair(pairCode);
    if (!pair) return { success: false, error: 'Pair not found' };
    
    const recipientToken = pair.device_token_a === requesterToken ? pair.device_token_b : pair.device_token_a;
    
    if (recipientToken) {
        const recipientSocket = connectedDevices.get(recipientToken);
        if (recipientSocket) recipientSocket.emit('alarm-stopped', { success: true });
        
        const webClient = webClients.get(recipientToken);
        if (webClient) webClient.emit('alarm-stopped', { success: true });
    }
    
    console.log('Alarm killed for pair ' + pairCode);
    return { success: true };
}

app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());

const limiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 100, message: { error: 'Too many requests' } });
app.use('/api/', limiter);

app.get('/', (req, res) => {
    res.json({ name: 'Stealth Calculator API', status: 'running', connectedDevices: connectedDevices.size + webClients.size });
});

app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString(), connectedDevices: connectedDevices.size + webClients.size });
});

app.post('/api/pair', async (req, res) => {
    try {
        const { pairCode, deviceToken, deviceName } = req.body;
        if (!pairCode || !deviceToken) return res.status(400).json({ error: 'pairCode and deviceToken required' });
        
        const result = await createPair(pairCode, deviceToken, deviceName || 'Unknown');
        if (!result.success) return res.status(400).json({ error: result.error });
        
        res.json({ success: true, pairId: result.pair.id, pairCode: result.pair.pair_code, isNew: result.isNew, status: result.isNew ? 'waiting_for_partner' : 'paired' });
    } catch (error) {
        console.error('Pair error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.post('/api/signal', async (req, res) => {
    try {
        const { pairCode, senderToken, alertMode } = req.body;
        if (!pairCode || !senderToken) return res.status(400).json({ error: 'pairCode and senderToken required' });
        
        const result = await triggerSignal(pairCode, senderToken, ['nudge', 'continuous'].includes(alertMode) ? alertMode : 'nudge');
        if (!result.success) return res.status(400).json({ error: result.error });
        res.json(result);
    } catch (error) {
        console.error('Signal error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.post('/api/kill', async (req, res) => {
    try {
        const { pairCode, requesterToken } = req.body;
        if (!pairCode || !requesterToken) return res.status(400).json({ error: 'pairCode and requesterToken required' });
        
        const result = await killAlarm(pairCode, requesterToken);
        if (!result.success) return res.status(400).json({ error: result.error });
        res.json(result);
    } catch (error) {
        console.error('Kill error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);
    
    socket.on('register', (data) => {
        const { deviceToken, pairCode, isWeb } = data;
        if (!deviceToken) return;
        
        if (isWeb) webClients.set(deviceToken, socket);
        else connectedDevices.set(deviceToken, socket);
        
        socket.deviceToken = deviceToken;
        socket.pairCode = pairCode;
        socket.isWeb = !!isWeb;
        socket.emit('registered', { success: true });
    });
    
    socket.on('send-signal', async (data) => {
        const { pairCode, alertMode } = data;
        const senderToken = socket.deviceToken;
        if (!senderToken || !pairCode) return;
        
        const result = await triggerSignal(pairCode, senderToken, alertMode || 'nudge');
        socket.emit('signal-result', result);
    });
    
    socket.on('kill-alarm', async (data) => {
        const { pairCode } = data;
        const requesterToken = socket.deviceToken;
        if (!requesterToken || !pairCode) return;
        
        const result = await killAlarm(pairCode, requesterToken);
        socket.emit('kill-result', result);
    });
    
    socket.on('disconnect', () => {
        if (socket.deviceToken) {
            if (socket.isWeb) webClients.delete(socket.deviceToken);
            else connectedDevices.delete(socket.deviceToken);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log('Stealth Calculator Server running on port ' + PORT);
});

process.on('SIGTERM', async () => {
    await pool.end();
    server.close(() => process.exit(0));
});