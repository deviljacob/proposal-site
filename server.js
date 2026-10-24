// Live Proposal Website — server.js
// Static file server + WebSocket room relay (rooms keyed by ?room= code).
// Max 2 players per room; roles "boy" and "girl", each claimable once.
// Relays position updates and phase events between the two clients.

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const PUBLIC_DIR = path.join(__dirname, 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.json': 'application/json',
};

const httpServer = http.createServer((req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    let filePath = url.pathname === '/' ? '/index.html' : url.pathname;
    filePath = path.normalize(path.join(PUBLIC_DIR, filePath));
    if (!filePath.startsWith(PUBLIC_DIR)) {
      res.writeHead(403); res.end('Forbidden'); return;
    }
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); res.end('Not found'); return; }
      res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
      res.end(data);
    });
  } catch (e) {
    res.writeHead(500); res.end('Server error');
  }
});

// ---------------- WebSocket rooms ----------------
const wss = new WebSocketServer({ server: httpServer });
const rooms = new Map(); // roomCode -> { roles: {boy: ws|null, girl: ws|null}, clients: Set<ws> }

function getRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, { roles: { boy: null, girl: null }, clients: new Set() });
  }
  return rooms.get(code);
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(room, obj, except) {
  for (const ws of room.clients) {
    if (ws !== except) send(ws, obj);
  }
}

function broadcastAll(room, obj) {
  for (const ws of room.clients) send(ws, obj);
}

function rolesState(room) {
  return { type: 'roles', boy: !!room.roles.boy, girl: !!room.roles.girl };
}

const PHASE_EVENTS = ['letter_open', 'letter_closed', 'plane_start', 'proposal', 'yes', 'retry'];

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.role = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'join') {
      const code = typeof msg.room === 'string' && msg.room ? msg.room : 'default';
      const role = msg.role;
      if (role !== 'boy' && role !== 'girl') { send(ws, { type: 'error', message: 'bad role' }); return; }
      const room = getRoom(code);
      // Max 2 players per room.
      if (room.roles.boy && room.roles.girl && room.roles[role] !== ws) {
        send(ws, { type: 'role_taken', role });
        return;
      }
      if (room.roles[role] && room.roles[role] !== ws) {
        send(ws, { type: 'role_taken', role });
        return;
      }
      // Leave any previous room first.
      if (ws.roomCode) {
        const old = rooms.get(ws.roomCode);
        if (old) {
          old.clients.delete(ws);
          if (ws.role && old.roles[ws.role] === ws) old.roles[ws.role] = null;
          broadcastAll(old, rolesState(old));
        }
      }
      ws.roomCode = code;
      ws.role = role;
      room.roles[role] = ws;
      room.clients.add(ws);
      send(ws, { type: 'role_ok', role });
      broadcastAll(room, rolesState(room));
      return;
    }

    if (!ws.roomCode) return;
    const room = rooms.get(ws.roomCode);
    if (!room) return;

    if (msg.type === 'pos') {
      // Relay position to the other player, tagged with sender role.
      broadcast(room, {
        type: 'pos', role: ws.role,
        x: msg.x, z: msg.z, ry: msg.ry, moving: !!msg.moving,
      }, ws);
      return;
    }

    if (msg.type === 'event' && PHASE_EVENTS.includes(msg.name)) {
      // Phase events go to BOTH clients (sender included) so both stay in sync.
      broadcastAll(room, { type: 'event', name: msg.name, from: ws.role });
      return;
    }
  });

  ws.on('close', () => {
    if (!ws.roomCode) return;
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    room.clients.delete(ws);
    if (ws.role && room.roles[ws.role] === ws) room.roles[ws.role] = null;
    broadcastAll(room, rolesState(room));
    broadcastAll(room, { type: 'peer_left', role: ws.role });
    if (room.clients.size === 0) rooms.delete(ws.roomCode);
  });
});

httpServer.listen(PORT, () => {
  console.log(`Proposal site listening on http://localhost:${PORT}/`);
});
