const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Active clients tracking: Map of client -> { id, color, x, y, vx, vy, hp }
const clients = new Map();
const sessions = new Map(); // persistent sessions mapping deviceUuid -> state
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
    ws.missedPings = 0;
    ws.on('pong', () => {
        ws.isAlive = true;
        ws.missedPings = 0;
    });

    const playerId = nextPlayerId++;
    const assignedColor = NEON_COLORS[playerId % NEON_COLORS.length];
    
    const playerState = {
        id: playerId,
        color: assignedColor,
        x: 1500,
        y: 1000,
        vx: 0,
        vy: 0,
        hp: 100,
        builds: { frost: 0, overload: 0, orbital: 0, fire: 0 },
        synergies: { frostfire: false, overloadOrbital: false, frostOverload: false }
    };

    console.log(`[Lobby] Temporary Player ${playerId} connected. Color: ${assignedColor}.`);

    // 1. Send welcome configurations to the new client
    ws.send(JSON.stringify({
        type: 'welcome',
        id: playerId,
        color: assignedColor,
        state: playerState
    }));

    // 5秒內若未發送 join 訊息，主動超時中斷以釋放資源
    const joinTimeout = setTimeout(() => {
        if (!clients.has(ws)) {
            console.log(`[Lobby] Closing connection: Join timeout (5s) for temporary Player ${playerId}.`);
            ws.close();
        }
    }, 5000);

    // Message routing
    ws.on('message', (message) => {
        ws.isAlive = true;
        ws.missedPings = 0;
        try {
            const data = JSON.parse(message);
            
            if (data.type === 'ping') {
                ws.isAlive = true;
                ws.missedPings = 0;
                // Return pong to measure rtt latency
                ws.send(JSON.stringify({ type: 'pong', time: data.time }));
            }
            else if (data.type === 'join') {
                clearTimeout(joinTimeout);
                const deviceUuid = data.deviceUuid;
                
                // 1. 檢查是否存在已有 Session 進行重連恢復
                let session = sessions.get(deviceUuid);
                if (session) {
                    console.log(`[Session] Player ${session.id} (device: ${deviceUuid}) reconnected. Restoring state.`);
                    
                    if (session.disconnectTimeout) {
                        clearTimeout(session.disconnectTimeout);
                        session.disconnectTimeout = null;
                    }
                    
                    // 關閉舊連線
                    if (session.socket && session.socket !== ws && session.socket.readyState === WebSocket.OPEN) {
                        try { session.socket.close(); } catch(e) {}
                        clients.delete(session.socket);
                    }
                    
                    // 恢復屬性
                    session.socket = ws;
                    playerState.id = session.id;
                    playerState.color = session.color;
                    playerState.x = session.x;
                    playerState.y = session.y;
                    playerState.vx = session.vx;
                    playerState.vy = session.vy;
                    playerState.hp = session.hp;
                    playerState.downed = session.downed;
                    playerState.builds = session.builds || { frost: 0, overload: 0, orbital: 0, fire: 0 };
                    playerState.synergies = session.synergies || { frostfire: false, overloadOrbital: false, frostOverload: false };
                    playerState.deviceUuid = deviceUuid;
                    
                    clients.set(ws, playerState);
                    
                    // 傳送 welcome back 包含正確的舊 ID 與舊色彩
                    ws.send(JSON.stringify({
                        type: 'welcome',
                        id: session.id,
                        color: session.color,
                        state: playerState,
                        isReconnect: true
                    }));
                    
                    // 同步大廳戰友給重連者
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
                    
                    // 廣播給其他戰友，使其清除 offline 斷線樣式
                    broadcast({
                        type: 'peer_join',
                        peer: playerState,
                        isReconnect: true
                    }, ws);
                    
                    updateHostSelection();
                    return;
                }
                
                // 2. 檢查房間是否已滿 (上限 3 人，只算 active/grace-period sessions 佔用個數)
                if (sessions.size >= 3) {
                    console.log(`[Lobby] Rejecting join from Player ${playerId}. Room is full (${sessions.size}/3).`);
                    ws.send(JSON.stringify({ type: 'error', message: 'Room full' }));
                    ws.close();
                    return;
                }

                console.log(`[Lobby] Player ${playerId} joined lobby. Active: ${clients.size + 1}/3`);
                
                // 3. Broadcast current existing players to the new client
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

                // 4. Register the new client
                playerState.deviceUuid = deviceUuid;
                clients.set(ws, playerState);
                
                // 註冊 Session 狀態
                sessions.set(deviceUuid, {
                    id: playerId,
                    color: assignedColor,
                    x: playerState.x,
                    y: playerState.y,
                    vx: playerState.vx,
                    vy: playerState.vy,
                    hp: playerState.hp,
                    downed: playerState.downed || false,
                    builds: playerState.builds || { frost: 0, overload: 0, orbital: 0, fire: 0 },
                    synergies: playerState.synergies || { frostfire: false, overloadOrbital: false, frostOverload: false },
                    stats: { damageDealt: 0, kills: 0, revives: 0 },
                    socket: ws,
                    disconnectTimeout: null
                });

                // 5. Notify existing clients that a new player joined
                broadcast({
                    type: 'peer_join',
                    peer: playerState
                }, ws);

                // 6. Update host selection
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
                    if (data.builds) state.builds = data.builds;
                    if (data.synergies) state.synergies = data.synergies;
                    
                    // 同步更新 session 中的位置以防斷線時備份
                    const session = sessions.get(state.deviceUuid);
                    if (session) {
                        session.x = data.x;
                        session.y = data.y;
                        session.vx = data.vx;
                        session.vy = data.vy;
                        session.hp = data.hp;
                        session.downed = data.downed;
                        if (data.builds) session.builds = data.builds;
                        if (data.synergies) session.synergies = data.synergies;
                    }
                    
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
                        aimAngle: state.aimAngle,
                        builds: state.builds,
                        synergies: state.synergies
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
                    const session = sessions.get(state.deviceUuid);
                    if (session) {
                        session.stats = {
                            damageDealt: data.damageDealt,
                            kills: data.kills,
                            revives: data.revives
                        };
                    }
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
        clearTimeout(joinTimeout);
        const state = clients.get(ws);
        if (state) {
            clients.delete(ws);
            
            // 房主中斷時，立刻遷移 Host Identity
            if (ws === currentHostSocket) {
                currentHostSocket = null;
                updateHostSelection();
            }
            
            const session = sessions.get(state.deviceUuid);
            if (session && session.socket === ws) {
                console.log(`[Session] Player ${state.id} disconnected. Keeping session for 15s.`);
                
                // 廣播給其他戰友該玩家暫時斷線中斷
                broadcast({
                    type: 'peer_offline',
                    id: state.id
                });
                
                // 啟動 15 秒寬限期
                session.disconnectTimeout = setTimeout(() => {
                    console.log(`[Session] Player ${state.id} session expired. Purging.`);
                    sessions.delete(state.deviceUuid);
                    
                    // 超時仍未重連，廣播 peer_leave 移除人物
                    broadcast({
                        type: 'peer_leave',
                        id: state.id
                    });
                }, 15000);
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
            ws.missedPings = (ws.missedPings || 0) + 1;
            console.log(`[Heartbeat] Missed ping count: ${ws.missedPings}/3 for Player ${clients.get(ws)?.id || 'unknown'}`);
            if (ws.missedPings >= 3) {
                console.log(`[Heartbeat] Terminating inactive connection for Player ${clients.get(ws)?.id || 'unknown'}`);
                return ws.terminate();
            }
        } else {
            ws.isAlive = false;
            ws.missedPings = 0;
        }
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
