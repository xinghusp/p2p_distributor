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
}

// 创建种子实例并初始化
const teacherSeed = new TeacherSeed();
teacherSeed.init();

// 将方法暴露给全局，以便在UI中使用
window.registerAsSeed = function(distributionId) {
    teacherSeed.registerAsSeed(distributionId);
};