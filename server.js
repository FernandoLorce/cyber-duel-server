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
const MAX_PLAYERS = 4;

function getDefaultGameState(diffValue = 0) {
    const depts = [
        { id: "1-1", breached: false },
        { id: "2-1", breached: false },
        { id: "2-2", breached: false },
        { id: "3-1", breached: false },
        { id: "3-2", breached: false },
        { id: "3-3", breached: false },
        { id: "3-4", breached: false },
        { id: "4-1", breached: false },
        { id: "4-2", breached: false },
        { id: "4-3", breached: false },
        { id: "4-4", breached: false },
        { id: "4-5", breached: false }
    ];
    
    if (diffValue >= 11 && diffValue <= 20) {
        depts[0].breached = true;
    } else if (diffValue >= 21 && diffValue <= 30) {
        depts[0].breached = true;
        depts[1].breached = true;
        depts[2].breached = true;
    }
    
    return {
        depts: depts,
        gameActive: false,
        timeLeft: 600,
        diffValue: diffValue,
        startTime: null,
        totalTime: 600,
        // ✅ 新增：攻击状态（服务器统一管理）
        breachAlerts: {}  // { deptId: { deptName, skill, timeLeft, startTime, totalTime } }
    };
}

function startRoomTimer(roomId) {
    const room = rooms.get(roomId);
    if (!room) return;
    if (room.gameTimer) {
        clearInterval(room.gameTimer);
        room.gameTimer = null;
    }
    
    room.gameState.gameActive = true;
    room.gameState.startTime = Date.now();
    room.gameState.timeLeft = room.gameState.totalTime;
    
    broadcastToRoom(roomId, {
        type: 'game_started',
        state: room.gameState
    });
    
    room.gameTimer = setInterval(() => {
        const room = rooms.get(roomId);
        if (!room) return;
        
        const elapsed = (Date.now() - room.gameState.startTime) / 1000;
        room.gameState.timeLeft = Math.max(0, room.gameState.totalTime - elapsed);
        
        // ✅ 同时更新所有攻击的剩余时间
        const now = Date.now();
        const expiredAlerts = [];
        for (const deptId in room.gameState.breachAlerts) {
            const alert = room.gameState.breachAlerts[deptId];
            const elapsed2 = (now - alert.startTime) / 1000;
            const remaining = Math.max(0, alert.totalTime - elapsed2);
            alert.timeLeft = remaining;
            
            // 检查是否已沦陷（攻击成功）
            const dept = room.gameState.depts.find(d => d.id === deptId);
            if (dept && dept.breached) {
                expiredAlerts.push(deptId);
            } else if (remaining <= 0) {
                // 攻击成功，标记沦陷
                if (dept) {
                    dept.breached = true;
                    dept._attackedBy = alert.skill;
                }
                expiredAlerts.push(deptId);
            }
        }
        
        // 清理已沦陷的攻击
        for (const deptId of expiredAlerts) {
            delete room.gameState.breachAlerts[deptId];
        }
        
        // ✅ 广播完整状态（包括攻击）
        broadcastToRoom(roomId, {
            type: 'state_sync',
            state: room.gameState
        });
        
        if (room.gameState.timeLeft <= 0) {
            clearInterval(room.gameTimer);
            room.gameTimer = null;
            room.gameState.gameActive = false;
            
            broadcastToRoom(roomId, {
                type: 'game_over'
            });
        }
    }, 200); // 200ms 更新频率，更流畅
}

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
                        const diff = data.diffValue || 0;
                        rooms.set(roomId, { 
                            clients: new Map(),
                            gameState: getDefaultGameState(diff),
                            gameTimer: null
                        });
                    }
                    
                    const room = rooms.get(roomId);
                    
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
                    
                    ws.send(JSON.stringify({
                        type: 'room_joined',
                        roomId: roomId,
                        clientId: clientId,
                        playerCount: room.clients.size,
                        maxPlayers: MAX_PLAYERS,
                        state: room.gameState
                    }));
                    
                    broadcastToRoom(roomId, {
                        type: 'room_update',
                        playerCount: room.clients.size,
                        maxPlayers: MAX_PLAYERS
                    });
                    
                    if (room.gameState.gameActive && !room.gameTimer) {
                        startRoomTimer(roomId);
                    }
                    break;
                }
                
                case 'start_game': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        const room = rooms.get(ws.roomId);
                        if (!room.gameState.gameActive) {
                            if (data.diffValue !== undefined) {
                                room.gameState.diffValue = data.diffValue;
                                room.gameState.depts = getDefaultGameState(data.diffValue).depts;
                            }
                            startRoomTimer(ws.roomId);
                        }
                    }
                    break;
                }
                
                case 'breach_alert': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        const room = rooms.get(ws.roomId);
                        const deptId = data.deptId;
                        
                        // ✅ 检查是否已有攻击
                        if (room.gameState.breachAlerts[deptId]) {
                            // 已有攻击，更新剩余时间
                            room.gameState.breachAlerts[deptId].timeLeft = data.timeLeft;
                            room.gameState.breachAlerts[deptId].startTime = Date.now();
                        } else {
                            // 新攻击
                            room.gameState.breachAlerts[deptId] = {
                                deptName: data.deptName,
                                skill: data.skill,
                                timeLeft: data.timeLeft,
                                startTime: Date.now(),
                                totalTime: data.timeLeft
                            };
                        }
                        
                        // ✅ 广播攻击状态
                        broadcastToRoom(ws.roomId, {
                            type: 'breach_alert',
                            deptId: deptId,
                            deptName: data.deptName,
                            skill: data.skill,
                            timeLeft: data.timeLeft,
                            breachAlerts: room.gameState.breachAlerts
                        });
                    }
                    break;
                }
                
                case 'breach_defended': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        const room = rooms.get(ws.roomId);
                        const deptId = data.deptId;
                        
                        // ✅ 清除攻击状态
                        delete room.gameState.breachAlerts[deptId];
                        
                        // 标记部门为未沦陷
                        const dept = room.gameState.depts.find(d => d.id === deptId);
                        if (dept) {
                            dept.breached = false;
                            delete dept._attackedBy;
                        }
                        
                        broadcastToRoom(ws.roomId, {
                            type: 'breach_defended',
                            deptId: deptId,
                            breachAlerts: room.gameState.breachAlerts
                        });
                    }
                    break;
                }
                
                case 'state_update': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        const room = rooms.get(ws.roomId);
                        // ✅ 只更新部门状态，不覆盖其他
                        if (data.state && data.state.depts) {
                            for (const s of data.state.depts) {
                                const dept = room.gameState.depts.find(d => d.id === s.id);
                                if (dept) {
                                    dept.breached = s.breached;
                                }
                            }
                        }
                        broadcastToRoom(ws.roomId, {
                            type: 'state_sync',
                            state: room.gameState
                        }, ws);
                    }
                    break;
                }
                
                case 'game_over': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        const room = rooms.get(ws.roomId);
                        if (room.gameTimer) {
                            clearInterval(room.gameTimer);
                            room.gameTimer = null;
                        }
                        room.gameState.gameActive = false;
                        broadcastToRoom(ws.roomId, {
                            type: 'game_over'
                        });
                    }
                    break;
                }
                
                case 'reset_game': {
                    if (ws.roomId && rooms.has(ws.roomId)) {
                        const room = rooms.get(ws.roomId);
                        if (room.gameTimer) {
                            clearInterval(room.gameTimer);
                            room.gameTimer = null;
                        }
                        room.gameState = getDefaultGameState(room.gameState.diffValue);
                        broadcastToRoom(ws.roomId, {
                            type: 'reset_game'
                        });
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
                if (room.gameTimer) {
                    clearInterval(room.gameTimer);
                    room.gameTimer = null;
                }
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
