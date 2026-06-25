const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
    // 健康检查
    if (req.url === '/health' || req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('WebSocket server is running');
        return;
    }
    res.writeHead(404);
    res.end('Not found');
});

const wss = new WebSocket.Server({ 
    server,
    // 添加这个选项允许所有连接
    perMessageDeflate: false
});

// 存储房间
const rooms = new Map();

wss.on('connection', (ws, req) => {
    console.log('🔗 新客户端连接');
    let clientRoom = null;
    let clientId = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('📩 收到消息:', data.type);
            
            switch(data.type) {
                case 'join_room': {
                    const roomId = data.roomId;
                    clientId = data.clientId || 'client_' + Date.now();
                    
                    if (!rooms.has(roomId)) {
                        rooms.set(roomId, { clients: new Map() });
                    }
                    
                    const room = rooms.get(roomId);
                    room.clients.set(clientId, ws);
                    ws.clientId = clientId;
                    ws.roomId = roomId;
                    
                    console.log(`✅ ${clientId} 加入房间 ${roomId} (${room.clients.size}/2)`);
                    
                    ws.send(JSON.stringify({
                        type: 'room_joined',
                        roomId: roomId,
                        clientId: clientId,
                        playerCount: room.clients.size
                    }));
                    
                    broadcastToRoom(roomId, {
                        type: 'room_update',
                        playerCount: room.clients.size
                    }, ws);
                    break;
                }
                
                case 'state_update': {
                    if (ws.roomId) {
                        broadcastToRoom(ws.roomId, {
                            type: 'state_update',
                            state: data.state
                        }, ws);
                    }
                    break;
                }
                
                case 'breach_alert': {
                    if (ws.roomId) {
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
                    if (ws.roomId) {
                        broadcastToRoom(ws.roomId, {
                            type: 'breach_defended',
                            deptId: data.deptId
                        }, ws);
                    }
                    break;
                }
                
                case 'game_over': {
                    if (ws.roomId) {
                        broadcastToRoom(ws.roomId, {
                            type: 'game_over'
                        }, ws);
                    }
                    break;
                }
                
                case 'reset_game': {
                    if (ws.roomId) {
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
            room.clients.delete(ws.clientId);
            
            if (room.clients.size === 0) {
                rooms.delete(ws.roomId);
                console.log(`🗑️ 房间 ${ws.roomId} 已销毁`);
            } else {
                broadcastToRoom(ws.roomId, {
                    type: 'room_update',
                    playerCount: room.clients.size
                });
                console.log(`👋 ${ws.clientId} 离开房间 ${ws.roomId} (${room.clients.size}/2)`);
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

// 重要：必须监听 0.0.0.0
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 WebSocket 服务器运行在端口 ${PORT}`);
    console.log(`📡 等待连接...`);
});