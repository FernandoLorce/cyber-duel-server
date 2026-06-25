const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('WebSocket server is running');
        return;
    }
    res.writeHead(404);
    res.end('Not found');
});

const wss = new WebSocket.Server({ server, perMessageDeflate: false });

const rooms = new Map();

// 游戏玩家数（红方+蓝方）
const MAX_PLAYERS = 4;
// 观战者人数
const MAX_SPECTATORS = 10;

wss.on('connection', (ws, req) => {
    console.log('🔗 新客户端连接');
    let clientId = null;
    let roomId = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('📩 收到消息:', data.type, '来自:', clientId || 'unknown');

            switch(data.type) {
                case 'join_room': {
                    roomId = data.roomId;
                    clientId = data.clientId || 'client_' + Date.now();
                    const isSpectator = data.isSpectator || false;
                    
                    if (!rooms.has(roomId)) {
                        rooms.set(roomId, { 
                            clients: new Map(),
                            players: new Map(),
                            spectators: new Map()
                        });
                    }
                    
                    const room = rooms.get(roomId);
                    
                    if (isSpectator) {
                        // 观战者
                        if (room.spectators.size >= MAX_SPECTATORS) {
                            ws.send(JSON.stringify({
                                type: 'error',
                                message: '观战席已满（最多' + MAX_SPECTATORS + '人）'
                            }));
                            return;
                        }
                        room.spectators.set(clientId, ws);
                        ws.isSpectator = true;
                        console.log(`👀 ${clientId} 加入观战 ${roomId} (${room.spectators.size}/${MAX_SPECTATORS})`);
                    } else {
                        // 玩家
                        if (room.clients.size >= MAX_PLAYERS) {
                            ws.send(JSON.stringify({
                                type: 'error',
                                message: '游戏已满（最多' + MAX_PLAYERS + '人），请选择观战'
                            }));
                            return;
                        }
                        room.clients.set(clientId, ws);
                        ws.isSpectator = false;
                        console.log(`✅ ${clientId} 加入房间 ${roomId} (${room.clients.size}/${MAX_PLAYERS})`);
                    }
                    
                    ws.clientId = clientId;
                    ws.roomId = roomId;
                    
                    // 发送给当前客户端
                    ws.send(JSON.stringify({
                        type: 'room_joined',
                        roomId: roomId,
                        clientId: clientId,
                        playerCount: room.clients.size,
                        spectatorCount: room.spectators.size,
                        maxPlayers: MAX_PLAYERS,
                        maxSpectators: MAX_SPECTATORS,
                        isSpectator: isSpectator
                    }));
                    
                    // 广播给所有人
                    broadcastToRoom(roomId, {
                        type: 'room_update',
                        playerCount: room.clients.size,
                        spectatorCount: room.spectators.size,
                        maxPlayers: MAX_PLAYERS,
                        maxSpectators: MAX_SPECTATORS
                    });
                    break;
                }
                
                case 'state_update': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        broadcastToRoom(ws.roomId, {
                            type: 'state_update',
                            state: data.state
                        }, ws);
                    }
                    break;
                }
                
                case 'breach_alert': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        broadcastToRoom(ws.roomId, {
                            type: 'breach_alert',
                            deptId: data.deptId,
                            deptName: data.deptName,
                            skill: data.skill,
                            timeLeft: data.timeLeft
                        }, ws);
                    }
                    break;
                }
                
                case 'breach_defended': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        broadcastToRoom(ws.roomId, {
                            type: 'breach_defended',
                            deptId: data.deptId
                        }, ws);
                    }
                    break;
                }
                
                case 'game_over': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        broadcastToRoom(ws.roomId, {
                            type: 'game_over'
                        }, ws);
                    }
                    break;
                }
                
                case 'reset_game': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        broadcastToRoom(ws.roomId, {
                            type: 'reset_game'
                        }, ws);
                    }
                    break;
                }
            }
        } catch(e) {
            console.error('❌ 消息处理错误:', e);
        }
    });

    ws.on('close', () => {
        if (ws.roomId && rooms.has(ws.roomId)) {
            const room = rooms.get(ws.roomId);
            
            if (ws.isSpectator) {
                room.spectators.delete(ws.clientId);
                console.log(`👋 ${ws.clientId} 离开观战 ${ws.roomId}`);
            } else {
                room.clients.delete(ws.clientId);
                console.log(`👋 ${ws.clientId} 离开房间 ${ws.roomId}`);
            }
            
            if (room.clients.size === 0 && room.spectators.size === 0) {
                rooms.delete(ws.roomId);
                console.log(`🗑️ 房间 ${ws.roomId} 已销毁`);
            } else {
                broadcastToRoom(ws.roomId, {
                    type: 'room_update',
                    playerCount: room.clients.size,
                    spectatorCount: room.spectators.size,
                    maxPlayers: MAX_PLAYERS,
                    maxSpectators: MAX_SPECTATORS
                });
            }
        }
    });
});

function broadcastToRoom(roomId, data, excludeWs = null) {
    if (!rooms.has(roomId)) return;
    
    const room = rooms.get(roomId);
    const message = JSON.stringify(data);
    
    // 发送给所有玩家
    room.clients.forEach((client) => {
        if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
    
    // 发送给所有观战者
    room.spectators.forEach((client) => {
        if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 WebSocket 服务器运行在端口 ${PORT}`);
    console.log(`📡 最大玩家数: ${MAX_PLAYERS}`);
    console.log(`📡 最大观战者: ${MAX_SPECTATORS}`);
});
