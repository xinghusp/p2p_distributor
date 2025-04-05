// 教师端种子节点注册脚本
class TeacherSeed {
    constructor() {
        this.ws = null;
        this.clientId = null;
        this.currentDistributionId = null;
        this.fileData = {};
        this.connected = false;
        this.heartbeatInterval = null;
        this.peerId = null;
        this.peerConnections = {};  // 存储所有P2P连接
    }

    // 初始化WebSocket连接
    init() {
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${wsProtocol}//${window.location.host}/ws/teacher`;

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            console.log('TeacherSeed: WebSocket连接已建立');
            this.connected = true;
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            console.log('TeacherSeed: 收到消息', data);

            if (data.type === 'welcome') {
                this.clientId = data.client_id;
                console.log(`TeacherSeed: 已连接，ID: ${this.clientId}`);

                // 如果已有分发任务ID，自动注册
                if (this.currentDistributionId) {
                    this.registerAsSeed(this.currentDistributionId);
                }
            } else if (data.type === 'announce_success') {
                this.peerId = data.peer_id;
                console.log(`TeacherSeed: 成功注册为种子节点，Peer ID: ${this.peerId}`);
                this.startHeartbeat(); // 注册成功后启动心跳
                this.loadFilesAndRegisterPieces(this.currentDistributionId);
            } else if (data.type === 'signaling') {
                // 处理WebRTC信令消息
                this.handleSignalingMessage(data.payload);
            } else if (data.type === 'heartbeat') {
                // 处理心跳消息
                console.debug('收到服务器心跳');
            } else if (data.type === 'error') {
                console.error(`TeacherSeed: 错误: ${data.message}`);
            }
        };

        this.ws.onclose = () => {
            console.log('TeacherSeed: WebSocket连接已关闭');
            this.connected = false;
            this.stopHeartbeat();
            // 5秒后重连
            setTimeout(() => this.init(), 5000);
        };
    }

    // 启动心跳
    startHeartbeat() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
        }

        this.heartbeatInterval = setInterval(() => {
            if (!this.connected || !this.peerId || !this.currentDistributionId) {
                return;
            }

            this.sendHeartbeat();
        }, 15000); // 15秒一次心跳

        console.log('TeacherSeed: 心跳机制已启动');
    }

    // 停止心跳
    stopHeartbeat() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }
    }

    // 发送心跳
    sendHeartbeat() {
        if (!this.connected || !this.ws || !this.peerId) {
            return;
        }

        this.ws.send(JSON.stringify({
            type: 'heartbeat',
            peer_id: this.peerId,
            distribution_id: this.currentDistributionId
        }));

        console.debug('TeacherSeed: 已发送心跳');
    }

    // 注册为种子节点
    registerAsSeed(distributionId) {
        if (!this.connected || !this.ws) {
            console.log('TeacherSeed: WebSocket未连接，无法注册种子节点');
            return;
        }

        this.currentDistributionId = distributionId;

        // 向Tracker注册自己
        this.ws.send(JSON.stringify({
            type: 'announce',
            distribution_id: distributionId,
            ip_address: window.location.hostname,
            port: 8000 + Math.floor(Math.random() * 1000), // 随机端口，仅用于标识
            is_seed: true, // 标记为种子节点
            user_agent: navigator.userAgent
        }));

        console.log(`TeacherSeed: 正在注册为分发任务 ${distributionId} 的种子节点`);
    }

    // 加载文件信息并注册所有文件片段
    async loadFilesAndRegisterPieces(distributionId) {
        try {
            // 获取分发任务详情
            const response = await fetch(`/api/distributions/${distributionId}`);
            const distribution = await response.json();

            if (distribution.files && distribution.files.length > 0) {
                // 遍历所有文件
                for (const file of distribution.files) {
                    // 获取文件片段信息
                    const piecesResponse = await fetch(`/api/files/${file.id}/pieces`);
                    const pieces = await piecesResponse.json();

                    // 将所有片段标记为已拥有
                    for (const piece of pieces) {
                        this.ws.send(JSON.stringify({
                            type: 'update_piece',
                            piece_id: piece.id,
                            has_piece: true
                        }));
                    }

                    console.log(`TeacherSeed: 已注册文件 ${file.path} 的 ${pieces.length} 个片段`);
                }

                console.log(`TeacherSeed: 已完成所有文件片段注册，分发任务 ${distributionId} 可以开始下载`);
            }
        } catch (error) {
            console.error(`TeacherSeed: 加载文件信息失败:`, error);
        }
    }

    handleSignalingMessage(payload) {
        if (!payload || !payload.type) return;

        console.log(`TeacherSeed: 收到信令消息: ${payload.type} 从节点 ${payload.source}`);

        const sourcePeerId = payload.source;

        if (!sourcePeerId) {
            console.error('信令消息没有源节点ID');
            return;
        }

        // 如果这是一个新的对等连接请求，初始化一个新的RTCPeerConnection
        if (!this.peerConnections[sourcePeerId]) {
            this.initializePeerConnection(sourcePeerId);
        }

        const peerConnection = this.peerConnections[sourcePeerId].connection;

        if (payload.type === 'offer') {
            this.handleOfferMessage(peerConnection, payload, sourcePeerId);
        } else if (payload.type === 'answer') {
            this.handleAnswerMessage(peerConnection, payload);
        } else if (payload.type === 'ice_candidate') {
            this.handleICECandidateMessage(peerConnection, payload);
        }
    }

// 初始化对等连接
    initializePeerConnection(peerId) {
        console.log(`TeacherSeed: 初始化与节点 ${peerId} 的连接`);

        // 配置ICE服务器
        const iceServers = [
            {urls: 'stun:stun.l.google.com:19302'}
        ];

        // 创建新的RTCPeerConnection
        const peerConnection = new RTCPeerConnection({iceServers});

        // 存储连接状态
        this.peerConnections[peerId] = {
            connection: peerConnection,
            id: peerId,
            connected: false,
            status: 'connecting',
            lastActivity: Date.now(),
            dataChannel: null
        };

        // 处理ICE候选
        peerConnection.onicecandidate = (event) => {
            if (event.candidate) {
                console.log(`TeacherSeed: 向节点 ${peerId} 发送ICE候选`);
                this.sendSignalingMessage({
                    type: 'ice_candidate',
                    candidate: event.candidate,
                    target: peerId,
                    source: this.peerId
                });
            }
        };

        // 监听连接状态变化
        peerConnection.onconnectionstatechange = () => {
            console.log(`TeacherSeed: 与节点 ${peerId} 的连接状态变为 ${peerConnection.connectionState}`);

            if (peerConnection.connectionState === 'connected') {
                this.peerConnections[peerId].connected = true;
                this.peerConnections[peerId].status = 'connected';
                console.log(`TeacherSeed: 与节点 ${peerId} 建立连接成功！`);
            } else if (peerConnection.connectionState === 'disconnected' ||
                peerConnection.connectionState === 'failed') {
                this.peerConnections[peerId].connected = false;
                this.peerConnections[peerId].status = 'disconnected';
                console.log(`TeacherSeed: 与节点 ${peerId} 的连接断开或失败`);
            }
        };

        // 处理数据通道
        peerConnection.ondatachannel = (event) => {
            console.log(`TeacherSeed: 收到来自节点 ${peerId} 的数据通道`);
            this.setupDataChannel(event.channel, peerId);
        };

        return peerConnection;
    }

// 处理offer消息
    handleOfferMessage(peerConnection, payload, sourcePeerId) {
        console.log(`TeacherSeed: 处理来自节点 ${sourcePeerId} 的offer`);

        // 设置远程描述
        peerConnection.setRemoteDescription(new RTCSessionDescription(payload.offer))
            .then(() => {
                // 创建answer
                return peerConnection.createAnswer();
            })
            .then(answer => {
                // 设置本地描述
                return peerConnection.setLocalDescription(answer);
            })
            .then(() => {
                // 发送answer到源节点
                this.sendSignalingMessage({
                    type: 'answer',
                    answer: peerConnection.localDescription,
                    target: sourcePeerId,
                    source: this.peerId
                });
                console.log(`TeacherSeed: 已发送answer给节点 ${sourcePeerId}`);
            })
            .catch(error => {
                console.error(`TeacherSeed: 处理offer失败: ${error}`);
            });
    }

// 处理answer消息
    handleAnswerMessage(peerConnection, payload) {
        console.log(`TeacherSeed: 处理来自节点 ${payload.source} 的answer`);

        peerConnection.setRemoteDescription(new RTCSessionDescription(payload.answer))
            .catch(error => {
                console.error(`TeacherSeed: 设置远程描述失败: ${error}`);
            });
    }

// 处理ICE候选消息
    handleICECandidateMessage(peerConnection, payload) {
        console.log(`TeacherSeed: 处理来自节点 ${payload.source} 的ICE候选`);

        try {
            peerConnection.addIceCandidate(new RTCIceCandidate(payload.candidate))
                .catch(error => {
                    console.error(`TeacherSeed: 添加ICE候选失败: ${error}`);
                });
        } catch (e) {
            console.error(`TeacherSeed: 处理ICE候选出错: ${e}`);
        }
    }

// 设置数据通道
    setupDataChannel(dataChannel, peerId) {
        this.peerConnections[peerId].dataChannel = dataChannel;

        dataChannel.onopen = () => {
            console.log(`TeacherSeed: 与节点 ${peerId} 的数据通道已打开`);
            this.peerConnections[peerId].connected = true;
            this.peerConnections[peerId].status = 'connected';
        };

        dataChannel.onclose = () => {
            console.log(`TeacherSeed: 与节点 ${peerId} 的数据通道已关闭`);
            this.peerConnections[peerId].connected = false;
            this.peerConnections[peerId].status = 'disconnected';
        };

        dataChannel.onerror = (error) => {
            console.error(`TeacherSeed: 数据通道错误: ${error}`);
        };

        dataChannel.onmessage = (event) => {
            this.handleDataChannelMessage(event, peerId);
        };
    }

// 发送信令消息
    sendSignalingMessage(message) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            console.error('TeacherSeed: WebSocket未连接，无法发送信令消息');
            return;
        }

        this.ws.send(JSON.stringify({
            type: 'signaling',
            payload: message
        }));

        console.log(`TeacherSeed: 发送信令消息: ${message.type} 给节点 ${message.target}`);
    }

// 处理数据通道消息
    handleDataChannelMessage(event, sourcePeerId) {
        // 更新最后活动时间
        if (this.peerConnections[sourcePeerId]) {
            this.peerConnections[sourcePeerId].lastActivity = Date.now();
        }

        // 处理文本消息
        if (typeof event.data === 'string') {
            try {
                const message = JSON.parse(event.data);

                if (message.type === 'piece_request') {
                    this.handlePieceRequest(message, sourcePeerId);
                } else {
                    console.log(`TeacherSeed: 收到未知类型消息: ${message.type}`);
                }
            } catch (e) {
                console.error(`TeacherSeed: 解析消息失败: ${e}`);
            }
        }
    }
}

// 创建种子实例并初始化
const teacherSeed = new TeacherSeed();
teacherSeed.init();

// 将方法暴露给全局，以便在UI中使用
window.registerAsSeed = function (distributionId) {
    teacherSeed.registerAsSeed(distributionId);
};