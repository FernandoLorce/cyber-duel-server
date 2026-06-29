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

// 最大玩家数
const MAX_PLAYERS = 8;

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
                    
                    if (!rooms.has(roomId)) {
                        rooms.set(roomId, { clients: new Map() });
                    }
                    
                    const room = rooms.get(roomId);
                    
                    // 清理无效的旧连接（断开但未清理的客户端）
                    room.clients.forEach(function(existingWs, existingClientId) {
                        if (existingWs.readyState !== WebSocket.OPEN) {
                            room.clients.delete(existingClientId);
                            console.log("Cleaning stale client: " + existingClientId);
                        }
                    });
                    
                    // 如果同一 clientId 已存在，替换旧连接（游戏结束后重新加入）
                    if (room.clients.has(clientId)) {
                        const oldWs = room.clients.get(clientId);
                        try { oldWs.close(); } catch(e) {}
                        room.clients.set(clientId, ws);
                        ws.clientId = clientId;
                        ws.roomId = roomId;
                        console.log("Rejoining room " + roomId + " (" + room.clients.size + "/" + MAX_PLAYERS + ")");
                        ws.send(JSON.stringify({
                            type: "room_joined",
                            roomId: roomId,
                            clientId: clientId,
                            playerCount: room.clients.size,
                            maxPlayers: MAX_PLAYERS
                        }));
                        broadcastToRoom(roomId, {
                            type: "room_update",
                            playerCount: room.clients.size,
                            maxPlayers: MAX_PLAYERS
                        });
                        return;
                    }
                    
                    // 检查房间是否已满（4人）
                    if (room.clients.size >= MAX_PLAYERS) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: '房间已满（最多4人）'
                        }));
                        return;
                    }
                    
                    room.clients.set(clientId, ws);
                    ws.clientId = clientId;
                    ws.roomId = roomId;
                    
                    console.log(`✅ ${clientId} 加入房间 ${roomId} (${room.clients.size}/${MAX_PLAYERS})`);
                    
                    // 发送给当前客户端
                    ws.send(JSON.stringify({
                        type: 'room_joined',
                        roomId: roomId,
                        clientId: clientId,
                        playerCount: room.clients.size,
                        maxPlayers: MAX_PLAYERS
                    }));
                    
                    // 广播给所有人（包括自己，更新人数）
                    broadcastToRoom(roomId, {
                        type: 'room_update',
                        playerCount: room.clients.size,
                        maxPlayers: MAX_PLAYERS
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
                
                case 'leave_room': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        const room = rooms.get(ws.roomId);
                        room.clients.delete(ws.clientId);
                        console.log(ws.clientId + " leaving room " + ws.roomId);
                        if (room.clients.size === 0) {
                            rooms.delete(ws.roomId);
                            console.log("Room " + ws.roomId + " destroyed");
                        } else {
                            broadcastToRoom(ws.roomId, {
                                type: 'room_update',
                                playerCount: room.clients.size,
                                maxPlayers: MAX_PLAYERS
                            });
                        }
                        ws.clientId = null;
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
            room.clients.delete(ws.clientId);
            
            if (room.clients.size === 0) {
                rooms.delete(ws.roomId);
                console.log(`🗑️ 房间 ${ws.roomId} 已销毁`);
            } else {
                broadcastToRoom(ws.roomId, {
                    type: 'room_update',
                    playerCount: room.clients.size,
                    maxPlayers: MAX_PLAYERS
                });
                console.log(`👋 ${ws.clientId} 离开房间 ${ws.roomId} (${room.clients.size}/${MAX_PLAYERS})`);
            }
        }
    });
});

function broadcastToRoom(roomId, data, excludeWs = null) {
    if (!rooms.has(roomId)) return;
    
    const room = rooms.get(roomId);
    const message = JSON.stringify(data);
    
    room.clients.forEach((client) => {
        if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 WebSocket 服务器运行在端口 ${PORT}`);
    console.log(`📡 最大玩家数: ${MAX_PLAYERS}`);
});
