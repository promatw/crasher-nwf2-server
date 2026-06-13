const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Active clients tracking: Map of client -> { id, color, x, y, vx, vy, hp }
const clients = new Map();
let nextPlayerId = 1;
let currentHostSocket = null;

const NEON_COLORS = ['#d800ff', '#ffdd00', '#00f3ff'];

console.log(`=============================================`);
console.log(` Crasher-NWF2 Production Multiplayer Server`);
console.log(` Listening on PORT: ${PORT}`);
console.log(`=============================================`);

// Helper: update and migrate Host selection dynamically
function updateHostSelection() {
    let minId = Infinity;
    let nextHostWs = null;
    
    clients.forEach((state, clientSocket) => {
        if (clientSocket.readyState === WebSocket.OPEN) {
            if (state.id < minId) {
                minId = state.id;
                nextHostWs = clientSocket;
            }
        }
    });

    if (nextHostWs && nextHostWs !== currentHostSocket) {
        currentHostSocket = nextHostWs;
        const hostState = clients.get(currentHostSocket);
        console.log(`[Host Migration] Player ${hostState.id} promoted to HOST.`);
        
        currentHostSocket.send(JSON.stringify({
            type: 'host_promote',
            id: hostState.id
        }));
        
        // Broadcast new host identity to all other peers
        broadcast({
            type: 'host_sync',
            hostId: hostState.id
        });
    }
}

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => {
        ws.isAlive = true;
    });

    // Limit lobby to maximum 3 players
    if (clients.size >= 3) {
        console.log(`[Lobby] Rejecting connection. Room is full (3/3).`);
        ws.send(JSON.stringify({ type: 'error', message: 'Room full' }));
        ws.close();
        return;
    }

    const playerId = nextPlayerId++;
    const assignedColor = NEON_COLORS[playerId % NEON_COLORS.length];
    
    const playerState = {
        id: playerId,
        color: assignedColor,
        x: 1500,
        y: 1000,
        vx: 0,
        vy: 0,
        hp: 100
    };

    console.log(`[Lobby] Player ${playerId} connected. Color: ${assignedColor}. Active: ${clients.size + 1}/3`);

    // 1. Send welcome configurations to the new client
    ws.send(JSON.stringify({
        type: 'welcome',
        id: playerId,
        color: assignedColor,
        state: playerState
    }));

    // Message routing
    ws.on('message', (message) => {
        ws.isAlive = true;
        try {
            const data = JSON.parse(message);
            
            if (data.type === 'ping') {
                ws.isAlive = true;
                // Return pong to measure rtt latency
                ws.send(JSON.stringify({ type: 'pong', time: data.time }));
            }
            else if (data.type === 'join') {
                const deviceUuid = data.deviceUuid;
                playerState.deviceUuid = deviceUuid;
                
                // 剔除相同 deviceUuid 的舊幽靈連線
                clients.forEach((state, clientSocket) => {
                    if (state.deviceUuid === deviceUuid && clientSocket !== ws) {
                        console.log(`[Lobby] Kicking duplicate device connection for Player ${state.id}`);
                        try { clientSocket.close(); } catch(e) {}
                        clients.delete(clientSocket);
                        broadcast({
                            type: 'peer_leave',
                            id: state.id
                        });
                    }
                });
                
                // 2. Broadcast current existing players to the new client
                const existingPeers = [];
                clients.forEach((state, clientSocket) => {
                    if (clientSocket.readyState === WebSocket.OPEN && clientSocket !== ws) {
                        existingPeers.push(state);
                    }
                });
                
                if (existingPeers.length > 0) {
                    ws.send(JSON.stringify({
                        type: 'sync_peers',
                        peers: existingPeers
                    }));
                }

                // 3. Register the new client
                clients.set(ws, playerState);

                // 4. Notify existing clients that a new player joined
                broadcast({
                    type: 'peer_join',
                    peer: playerState
                }, ws);

                // 5. Update host selection
                updateHostSelection();
            }
            else if (data.type === 'move') {
                // Update local state copy
                const state = clients.get(ws);
                if (state) {
                    state.x = data.x;
                    state.y = data.y;
                    state.vx = data.vx;
                    state.vy = data.vy;
                    state.hp = data.hp;
                    state.downed = data.downed;
                    state.aimActive = data.aimActive;
                    state.aimAngle = data.aimAngle;
                    
                    // Relay position updates to all other peers
                    broadcast({
                        type: 'peer_move',
                        id: state.id,
                        color: state.color,
                        x: state.x,
                        y: state.y,
                        vx: state.vx,
                        vy: state.vy,
                        hp: state.hp,
                        downed: state.downed,
                        aimActive: state.aimActive,
                        aimAngle: state.aimAngle
                    }, ws);
                }
            }
            else if (data.type === 'slash') {
                const state = clients.get(ws);
                if (state) {
                    // Relay vector slash strokes to peers
                    broadcast({
                        type: 'peer_slash',
                        id: state.id,
                        x1: data.x1,
                        y1: data.y1,
                        x2: data.x2,
                        y2: data.y2
                    }, ws);
                }
            }
            else if (data.type === 'enemy_spawn') {
                // Relay host's enemy spawn events to all peers
                broadcast(data, ws);
            }
            else if (data.type === 'enemy_update') {
                // Relay host's Kaiju updates to non-host clients
                broadcast(data, ws);
            }
            else if (data.type === 'stage_sync') {
                // Relay host's stage sync event to all peers
                broadcast(data, ws);
            }
            else if (data.type === 'game_restart') {
                // Relay game restart event to all peers
                broadcast(data, ws);
            }
            else if (data.type === 'request_restart') {
                // Forward restart request to the current host
                if (currentHostSocket && currentHostSocket.readyState === WebSocket.OPEN) {
                    currentHostSocket.send(JSON.stringify({ type: 'request_restart' }));
                }
            }
            else if (data.type === 'lightning_slash') {
                // Relay lightning slash points to all other peers
                const state = clients.get(ws);
                if (state) {
                    broadcast({
                        type: 'peer_lightning_slash',
                        id: state.id,
                        lines: data.lines
                    }, ws);
                }
            }
            else if (data.type === 'enemy_hit') {
                // Relay non-host hit event to host for authoritative calculations
                if (currentHostSocket && currentHostSocket.readyState === WebSocket.OPEN) {
                    currentHostSocket.send(JSON.stringify({
                        type: 'enemy_hit',
                        enemyId: data.enemyId,
                        damage: data.damage
                    }));
                }
            }
            else if (data.type === 'bullet_spawn') {
                // Relay host's enemy bullet spawn to all peers (preserve all fields like radius, isBlue)
                broadcast(data, ws);
            }
            else if (data.type === 'player_revived') {
                // Broadcast proximity revive success
                broadcast({
                    type: 'player_revived',
                    targetId: data.targetId
                }, ws);
            }
            else if (data.type === 'quick_signal') {
                // Relay quick signal radar pulses
                broadcast({
                    type: 'quick_signal',
                    x: data.x,
                    y: data.y,
                    id: data.id
                }, ws);
            }
            else if (data.type === 'player_stats_report') {
                const state = clients.get(ws);
                if (state) {
                    broadcast({
                        type: 'player_stats_report',
                        playerId: state.id,
                        damageDealt: data.damageDealt,
                        kills: data.kills,
                        revives: data.revives
                    }, ws);
                }
            }
        } catch (e) {
            console.warn(`[Error] Failed to process incoming message:`, e);
        }
    });

    // Cleanup on disconnect
    ws.on('close', () => {
        const state = clients.get(ws);
        if (state) {
            clients.delete(ws);
            console.log(`[Lobby] Player ${state.id} disconnected. Active: ${clients.size}/3`);
            
            // Notify other peers to purge this player instance
            broadcast({
                type: 'peer_leave',
                id: state.id
            });
            
            // If the disconnected client was the host, migrate host identity
            if (ws === currentHostSocket) {
                currentHostSocket = null;
                updateHostSelection();
            }
        }
    });

    ws.on('error', (err) => {
        console.error(`[Socket Error] Player error:`, err.message);
    });
});

// Helper: broadcast message to all clients, optionally excluding one sender socket
function broadcast(data, excludeWs = null) {
    const payload = JSON.stringify(data);
    clients.forEach((state, clientSocket) => {
        if (clientSocket !== excludeWs && clientSocket.readyState === WebSocket.OPEN) {
            clientSocket.send(payload);
        }
    });
}

// WebSocket Heartbeat keepalive check at 5-second intervals with active pings
const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log(`[Heartbeat] Terminating inactive connection for Player ${clients.get(ws)?.id || 'unknown'}`);
            return ws.terminate();
        }
        ws.isAlive = false;
        try {
            ws.ping(); // Send active protocol-level ping
        } catch (e) {
            ws.terminate();
        }
    });
}, 5000);

wss.on('close', () => {
    clearInterval(heartbeatInterval);
});
