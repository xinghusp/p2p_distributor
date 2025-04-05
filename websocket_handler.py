"""WebSocket通信处理"""
import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any

from fastapi import WebSocket, WebSocketDisconnect, Depends
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, Peer, db_operation
from tracker import P2PTracker

class ConnectionManager:
    """WebSocket连接管理器"""
    
    def __init__(self):
        """初始化连接管理器"""
        self.active_connections: Dict[str, WebSocket] = {}
        self.client_data: Dict[str, Dict[str, Any]] = {}
        self.tracker = None
    
    def set_tracker(self, tracker: P2PTracker):
        """设置追踪器实例"""
        self.tracker = tracker
    
    async def connect(self, websocket: WebSocket, client_id: str = None) -> str:
        """
        处理新WebSocket连接
        
        参数:
            websocket: WebSocket连接
            client_id: 客户端ID（可选）
            
        返回:
            str: 客户端ID
        """
        # 如果没有提供客户端ID，生成一个新的
        if not client_id:
            client_id = str(uuid.uuid4())
        
        # 接受WebSocket连接
        await websocket.accept()
        
        # 保存连接
        self.active_connections[client_id] = websocket
        self.client_data[client_id] = {
            "connected_at": asyncio.get_event_loop().time(),
            "distribution_id": None,
            "peer_id": None
        }
        
        # 如果追踪器已设置，注册连接
        if self.tracker:
            await self.tracker.register_connection(client_id, websocket)
        
        # 发送欢迎消息
        await self.send_json(client_id, {
            "type": "welcome",
            "client_id": client_id,
            "message": "成功连接到P2P分发服务器"
        })
        
        logging.info(f"客户端 {client_id} 已连接")
        return client_id
    
    async def disconnect(self, client_id: str):
        """
        处理WebSocket断开连接
        
        参数:
            client_id: 客户端ID
        """
        # 从活动连接中移除
        if client_id in self.active_connections:
            self.active_connections.pop(client_id)
        
        # 从客户端数据中移除
        if client_id in self.client_data:
            self.client_data.pop(client_id)
        
        # 如果追踪器已设置，注销连接
        if self.tracker:
            await self.tracker.unregister_connection(client_id)
        
        logging.info(f"客户端 {client_id} 已断开连接")
    
    async def send_text(self, client_id: str, message: str):
        """
        发送文本消息
        
        参数:
            client_id: 客户端ID
            message: 消息内容
        """
        if client_id in self.active_connections:
            await self.active_connections[client_id].send_text(message)
    
    async def send_json(self, client_id: str, data: Dict[str, Any]):
        """
        发送JSON消息
        
        参数:
            client_id: 客户端ID
            data: JSON数据
        """
        if client_id in self.active_connections:
            await self.active_connections[client_id].send_json(data)
    
    async def broadcast(self, message: str):
        """
        广播文本消息给所有连接
        
        参数:
            message: 消息内容
        """
        for connection in self.active_connections.values():
            await connection.send_text(message)
    
    async def broadcast_json(self, data: Dict[str, Any]):
        """
        广播JSON消息给所有连接
        
        参数:
            data: JSON数据
        """
        for connection in self.active_connections.values():
            await connection.send_json(data)
    
    async def broadcast_to_distribution(self, distribution_id: str, data: Dict[str, Any]):
        """
        广播JSON消息给特定分发任务的所有连接
        
        参数:
            distribution_id: 分发任务ID
            data: JSON数据
        """
        for client_id, client_info in self.client_data.items():
            if client_info.get("distribution_id") == distribution_id:
                await self.send_json(client_id, data)

# 创建全局连接管理器实例
manager = ConnectionManager()

async def handle_ws_message(websocket: WebSocket, client_id: str, db: AsyncSession):
    """
    处理WebSocket消息
    
    参数:
        websocket: WebSocket连接
        client_id: 客户端ID
        db: 数据库会话
    """
    try:
        # 循环处理消息
        while True:
            # 接收JSON消息
            data = await websocket.receive_json()
            
            # 提取消息类型
            message_type = data.get("type")
            if not message_type:
                await manager.send_json(client_id, {
                    "type": "error",
                    "message": "消息缺少type字段"
                })
                continue
            
            # 根据消息类型处理
            if message_type == "join_distribution":
                await handle_join_distribution(client_id, data, db)
            elif message_type == "announce":
                await handle_announce(client_id, data, db)
            elif message_type == "update_piece":
                await handle_update_piece(client_id, data, db)
            elif message_type == "request_peers":
                await handle_request_peers(client_id, data, db)
            elif message_type == "request_pieces":
                await handle_request_pieces(client_id, data, db)
            elif message_type == "ping":
                await manager.send_json(client_id, {
                    "type": "pong",
                    "time": data.get("time")
                })
            elif message_type == "heartbeat":  # 添加心跳处理
                await handle_heartbeat(client_id, data)
            else:
                await manager.send_json(client_id, {
                    "type": "error",
                    "message": f"未知消息类型: {message_type}"
                })
    except WebSocketDisconnect:
        await manager.disconnect(client_id)
    except Exception as e:
        logging.error(f"处理WebSocket消息出错: {e}", exc_info=True)
        await manager.disconnect(client_id)

async def handle_join_distribution(client_id: str, data: Dict[str, Any], db: AsyncSession):
    """
    处理加入分发任务请求
    
    参数:
        client_id: 客户端ID
        data: 消息数据
        db: 数据库会话
    """
    distribution_id = data.get("distribution_id")
    
    if not distribution_id:
        await manager.send_json(client_id, {
            "type": "error",
            "message": "缺少distribution_id参数"
        })
        return
    
    try:
        # 获取分发任务状态
        distribution_status = await manager.tracker.get_distribution_status(distribution_id)
        
        # 存储分发任务ID
        if client_id in manager.client_data:
            manager.client_data[client_id]["distribution_id"] = distribution_id
        
        # 返回成功消息
        await manager.send_json(client_id, {
            "type": "join_success",
            "distribution": distribution_status
        })
    except Exception as e:
        logging.error(f"加入分发任务失败: {e}", exc_info=True)
        await manager.send_json(client_id, {
            "type": "error",
            "message": f"加入分发任务失败: {str(e)}"
        })


# 在ConnectionManager类的handle_announce方法中修改
async def handle_announce(client_id: str, data: Dict[str, Any], db: AsyncSession):
    """
    处理节点通告消息

    参数:
        client_id: 客户端ID
        data: 消息数据
        db: 数据库会话
    """
    try:
        distribution_id = data.get("distribution_id")
        ip_address = data.get("ip_address")
        port = data.get("port")
        user_agent = data.get("user_agent", "Unknown")
        is_seed = data.get("is_seed", False)  # 是否是种子节点

        if not distribution_id or not ip_address or not port:
            await manager.send_json(client_id, {
                "type": "error",
                "message": "缺少必要的通告参数"
            })
            return

        # 注册节点
        peer_info = await manager.tracker.announce_peer(
            distribution_id, ip_address, port,client_id, user_agent, is_seed)

        # 保存peer_id到客户端数据
        if client_id in manager.client_data:
            manager.client_data[client_id]["peer_id"] = peer_info["peer_id"]

        # 向客户端发送成功消息
        await manager.send_json(client_id, {
            "type": "announce_success",
            "peer_id": peer_info["peer_id"]
        })

        # 如果是种子节点，记录日志
        if is_seed:
            logging.info(f"种子节点已注册: {client_id} (分发任务: {distribution_id})")

    except Exception as e:
        logging.error(f"处理announce消息时出错: {e}", exc_info=True)
        await manager.send_json(client_id, {
            "type": "error",
            "message": f"处理announce消息时出错: {str(e)}"
        })

async def handle_update_piece(client_id: str, data: Dict[str, Any], db: AsyncSession):
    """
    处理分片状态更新

    参数:
        client_id: 客户端ID
        data: 消息数据
        db: 数据库会话
    """
    piece_id = data.get("piece_id")
    has_piece = data.get("has_piece", True)

    if not piece_id:
        await manager.send_json(client_id, {
            "type": "error",
            "message": "缺少piece_id参数"
        })
        return

    # 获取节点ID
    peer_id = manager.client_data.get(client_id, {}).get("peer_id")
    if not peer_id:
        await manager.send_json(client_id, {
            "type": "error",
            "message": "节点未注册，请先调用announce"
        })
        return

    try:
        # 更新分片状态
        await manager.tracker.update_peer_piece(peer_id, piece_id, has_piece)

        # 返回成功消息
        await manager.send_json(client_id, {
            "type": "update_piece_success",
            "piece_id": piece_id
        })
    except Exception as e:
        logging.error(f"更新分片状态失败: {e}", exc_info=True)
        await manager.send_json(client_id, {
            "type": "error",
            "message": f"更新分片状态失败: {str(e)}"
        })

async def handle_request_peers(client_id: str, data: Dict[str, Any], db: AsyncSession):
    """
    处理请求节点列表

    参数:
        client_id: 客户端ID
        data: 消息数据
        db: 数据库会话
    """
    max_peers = data.get("max_peers", 20)

    # 获取节点ID和分发任务ID
    peer_id = manager.client_data.get(client_id, {}).get("peer_id")
    distribution_id = manager.client_data.get(client_id, {}).get("distribution_id")

    if not all([peer_id, distribution_id]):
        await manager.send_json(client_id, {
            "type": "error",
            "message": "节点未注册或未加入分发任务"
        })
        return

    try:
        # 获取节点列表
        peers = await manager.tracker.get_peers(distribution_id, peer_id, max_peers)

        # 返回节点列表
        await manager.send_json(client_id, {
            "type": "peers_list",
            "peers": peers
        })
    except Exception as e:
        logging.error(f"请求节点列表失败: {e}", exc_info=True)
        await manager.send_json(client_id, {
            "type": "error",
            "message": f"请求节点列表失败: {str(e)}"
        })

async def handle_request_pieces(client_id: str, data: Dict[str, Any], db: AsyncSession):
    """
    处理请求分片建议

    参数:
        client_id: 客户端ID
        data: 消息数据
        db: 数据库会话
    """
    max_suggestions = data.get("max_suggestions", 20)

    # 获取节点ID和分发任务ID
    peer_id = manager.client_data.get(client_id, {}).get("peer_id")
    distribution_id = manager.client_data.get(client_id, {}).get("distribution_id")

    if not all([peer_id, distribution_id]):
        await manager.send_json(client_id, {
            "type": "error",
            "message": "节点未注册或未加入分发任务"
        })
        return

    try:
        # 获取分片建议
        suggestions = await manager.tracker.get_piece_suggestions(distribution_id, peer_id, max_suggestions)

        # 返回分片建议
        await manager.send_json(client_id, {
            "type": "piece_suggestions",
            "suggestions": suggestions
        })
    except Exception as e:
        logging.error(f"请求分片建议失败: {e}", exc_info=True)
        await manager.send_json(client_id, {
            "type": "error",
            "message": f"请求分片建议失败: {str(e)}"
        })


# 在 ConnectionManager 类中添加处理心跳的方法
async def handle_heartbeat(client_id: str, message: Dict[str, Any]) -> None:
    """处理客户端的心跳消息"""
    try:
        peer_id = message.get("peer_id")
        distribution_id = message.get("distribution_id")

        if not peer_id or not distribution_id:
            await manager.send_json(client_id, {
                "type": "error",
                "message": f"心跳消息缺少必要参数"
            })
            return

        # 更新节点的 last_seen 时间戳
        async def update_last_seen(session):
            stmt = update(Peer).where(Peer.id == peer_id).values(last_seen=datetime.utcnow())
            await session.execute(stmt)
            return True

        success = await db_operation(update_last_seen)

        # 回复心跳消息
        await manager.send_json(client_id, {
            "type": "heartbeat_ack",
            "timestamp": datetime.utcnow().isoformat()
        })

        # 记录调试日志
        logging.debug(f"收到并处理了节点 {peer_id} 的心跳 (分发任务: {distribution_id})")

    except Exception as e:
        logging.error(f"处理心跳消息失败: {e}", exc_info=True)
        # 不向客户端发送错误，因为这可能会导致循环
# WebSocket路由处理函数
async def websocket_endpoint(websocket: WebSocket, client_id: str = None, db: AsyncSession = Depends(get_db)):
    """
    WebSocket端点处理函数

    参数:
        websocket: WebSocket连接
        client_id: 客户端ID（可选）
        db: 数据库会话
    """
    # 连接WebSocket
    client_id = await manager.connect(websocket, client_id)

    # 处理WebSocket消息
    await handle_ws_message(websocket, client_id, db)
