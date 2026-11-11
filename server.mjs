import express from 'express';
import { TikTokLiveConnection, WebcastEvent } from 'tiktok-live-connector';

const app = express();
const PORT = Number(process.env.PORT || 10000);
const TIKTOK_USERNAME = (process.env.TIKTOK_USERNAME || '').replace(/^@/, '').trim();
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const clients = new Set();
const seenUsers = new Set();
let connection = null;
let connectionStatus = 'waiting_for_username';
let lastError = null;

function send(client, event, data) {
  client.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}
function broadcast(event, data) {
  for (const client of clients) send(client, event, data);
}

app.get('/', (_req, res) => res.type('text').send('HORA LIVE BY CMR comment bridge is running.'));
app.get('/health', (_req, res) => res.json({
  ok: true,
  service: 'HORA LIVE BY CMR',
  connectionStatus,
  tiktokUsername: TIKTOK_USERNAME || null,
  connected: Boolean(connection?.isConnected),
  lastError
}));
app.get('/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();
  clients.add(res);
  send(res, 'status', { connectionStatus, connected: Boolean(connection?.isConnected) });
  req.on('close', () => clients.delete(res));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`HORA bridge listening on ${PORT}`);
  if (!TIKTOK_USERNAME) {
    connectionStatus = 'missing_tiktok_username';
    console.log('Set TIKTOK_USERNAME in the hosting service environment variables.');
    return;
  }
  connectToTikTok();
});

async function connectToTikTok() {
  connectionStatus = 'connecting';
  lastError = null;
  broadcast('status', { connectionStatus });
  try {
    connection = new TikTokLiveConnection(TIKTOK_USERNAME,{ processInitialData: false});
    connection.on(WebcastEvent.CHAT, (data) => {
      const comment = String(data?.comment || '').trim();
      if (comment.toLocaleLowerCase() !== '@hora') return;

      const user = data?.user || {};
      const uniqueId = String(user.uniqueId || '').trim();
      const userId = String(user.userId || '').trim();
      const dedupeKey = userId || uniqueId.toLocaleLowerCase();
      if (!dedupeKey || seenUsers.has(dedupeKey)) return;
      seenUsers.add(dedupeKey);

      const participant = {
        username: uniqueId ? `@${uniqueId.replace(/^@/, '')}` : String(user.nickname || 'Invitat'),
        nickname: String(user.nickname || uniqueId || 'Invitat'),
        comment: '@hora',
        joinedAt: Date.now()
      };
      console.log('HORA entry:', participant.username);
      broadcast('hora', participant);
    });
    connection.on('connected', () => {
      connectionStatus = 'connected';
      broadcast('status', { connectionStatus, connected: true });
    });
    connection.on('disconnected', () => {
      connectionStatus = 'disconnected';
      broadcast('status', { connectionStatus, connected: false });
    });
    connection.on('error', (error) => {
      lastError = String(error?.message || error);
      connectionStatus = 'error';
      console.error('TikTok connection error:', lastError);
      broadcast('status', { connectionStatus, connected: false, error: lastError });
    });
    await connection.connect();
    connectionStatus = 'connected';
    broadcast('status', { connectionStatus, connected: true });
  } catch (error) {
    lastError = String(error?.message || error);
    connectionStatus = 'error';
    console.error('Could not connect to TikTok LIVE:', lastError);
    broadcast('status', { connectionStatus, connected: false, error: lastError });
  }
}
