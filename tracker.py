"""P2P Tracker核心逻辑"""
import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple

from sqlalchemy import select, update, func, Integer, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from database import Distribution, FileInfo, FilePiece, Peer, PeerPiece, TrackerEvent, db_operation


class P2PTracker:
    """P2P追踪器实现"""
    
    def __init__(self, db_session: AsyncSession):
        """
        初始化追踪器

        参数:
            db_session: 数据库会话
        """
        self.db_session = db_session
        # 活动连接字典 {client_id: websocket}
        self.active_connections = {}
        # 分发任务字典 {distribution_id: Distribution}
        self.distributions = {}
        # 分片可用性缓存 {distribution_id: {piece_id: [peer_id1, peer_id2, ...]}}
        self._piece_availability = {}
        # 稀有度缓存 {distribution_id: {piece_id: rarity_score}}
        self._piece_rarity = {}
        
        self._cache_refresh_task = None
        self._heartbeat_task = None

    # 在 P2PTracker 类中添加这个方法
    async def _count_peers(self, distribution_id: str, session) -> int:
        """
        统计分发任务的节点数量

        参数:
            distribution_id: 分发任务ID
            session: 数据库会话

        返回:
            int: 节点数量
        """
        stmt = select(func.count()).select_from(Peer).where(
            Peer.distribution_id == distribution_id,
            Peer.last_seen > (datetime.utcnow() - timedelta(minutes=5))  # 只计算5分钟内活跃的节点
        )
        result = await session.execute(stmt)
        return result.scalar() or 0
    
    async def start(self):
        """启动追踪器服务"""
        logging.info("启动P2P追踪器...")
        
        # 从数据库加载活动分发任务
        await self._load_active_distributions()
        
        # 启动缓存刷新任务
        self._cache_refresh_task = asyncio.create_task(self._refresh_cache_periodically())
        
        # 启动心跳任务
        self._heartbeat_task = asyncio.create_task(self._send_heartbeats_periodically())
        
        logging.info("P2P追踪器已启动")
    
    async def stop(self):
        """停止追踪器服务"""
        logging.info("停止P2P追踪器...")
        
        # 取消后台任务
        if self._cache_refresh_task:
            self._cache_refresh_task.cancel()
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        
        # 关闭所有WebSocket连接
        for connection in self.active_connections.values():
            await connection.close()
        
        self.active_connections.clear()
        logging.info("P2P追踪器已停止")
    
    async def _load_active_distributions(self):
        """从数据库加载活动分发任务"""
        stmt = select(Distribution).where(Distribution.status == "active")
        result = await self.db_session.execute(stmt)
        active_distributions = result.scalars().all()
        
        for dist in active_distributions:
            self.distributions[dist.id] = dist
            # 初始化分片可用性缓存
            await self._initialize_piece_availability(dist.id)
    
    async def _initialize_piece_availability(self, distribution_id: str):
        """
        初始化分片可用性缓存
        
        参数:
            distribution_id: 分发任务ID
        """
        self._piece_availability[distribution_id] = {}
        self._piece_rarity[distribution_id] = {}
        
        # 查询所有拥有分片的节点
        stmt = (
            select(PeerPiece.piece_id, PeerPiece.peer_id)
            .join(Peer, Peer.id == PeerPiece.peer_id)
            .where(
                Peer.distribution_id == distribution_id,
                PeerPiece.has_piece == True
            )
        )
        result = await self.db_session.execute(stmt)
        peer_pieces = result.all()
        
        # 构建分片可用性缓存
        for piece_id, peer_id in peer_pieces:
            if piece_id not in self._piece_availability[distribution_id]:
                self._piece_availability[distribution_id][piece_id] = []
            self._piece_availability[distribution_id][piece_id].append(peer_id)
        
        # 计算初始稀有度
        await self._update_piece_rarity(distribution_id)
    
    async def _refresh_cache_periodically(self, interval: int = 30):
        """
        定期刷新缓存
        
        参数:
            interval: 刷新间隔(秒)
        """
        while True:
            try:
                await asyncio.sleep(interval)
                for dist_id in self.distributions:
                    await self._update_piece_availability(dist_id)
                    await self._update_piece_rarity(dist_id)
                    await self._cleanup_stale_peers(dist_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"缓存刷新出错: {e}")
    
    async def _send_heartbeats_periodically(self, interval: int = 30):
        """
        定期向客户端发送心跳
        
        参数:
            interval: 心跳间隔(秒)
        """
        while True:
            try:
                await asyncio.sleep(interval)
                timestamp = datetime.now().isoformat()
                
                # 向所有活动连接发送心跳
                for client_id, connection in self.active_connections.items():
                    try:
                        await connection.send_json({
                            "type": "heartbeat",
                            "timestamp": timestamp
                        })
                    except Exception as e:
                        logging.warning(f"向客户端 {client_id} 发送心跳失败: {e}")
                        # 移除失败的连接
                        self.active_connections.pop(client_id, None)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"发送心跳出错: {e}")
    
    async def _update_piece_availability(self, distribution_id: str):
        """
        更新分片可用性缓存
        
        参数:
            distribution_id: 分发任务ID
        """
        # 查询所有拥有分片的节点
        stmt = (
            select(PeerPiece.piece_id, PeerPiece.peer_id)
            .join(Peer, Peer.id == PeerPiece.peer_id)
            .where(
                Peer.distribution_id == distribution_id,
                PeerPiece.has_piece == True,
                Peer.last_seen > datetime.now() - timedelta(minutes=5)  # 仅考虑最近活跃的节点
            )
        )
        result = await self.db_session.execute(stmt)
        peer_pieces = result.all()
        
        # 重建分片可用性缓存
        self._piece_availability[distribution_id] = {}
        for piece_id, peer_id in peer_pieces:
            if piece_id not in self._piece_availability[distribution_id]:
                self._piece_availability[distribution_id][piece_id] = []
            self._piece_availability[distribution_id][piece_id].append(peer_id)
    
    async def _update_piece_rarity(self, distribution_id: str):
        """
        更新分片稀有度缓存
        
        参数:
            distribution_id: 分发任务ID
        """
        if distribution_id not in self._piece_availability:
            return
        
        # 查询该分发任务的所有分片
        stmt = (
            select(FilePiece.id)
            .join(FileInfo, FileInfo.id == FilePiece.file_id)
            .where(FileInfo.distribution_id == distribution_id)
        )
        result = await self.db_session.execute(stmt)
        all_pieces = result.scalars().all()
        
        # 计算每个分片的可用性
        piece_availability = self._piece_availability.get(distribution_id, {})
        total_peers = len(set(peer_id for peers in piece_availability.values() for peer_id in peers))
        
        if total_peers == 0:
            # 没有节点有分片，所有分片都是稀有的
            self._piece_rarity[distribution_id] = {piece_id: 1.0 for piece_id in all_pieces}
        else:
            # 计算稀有度分数 (1.0表示最稀有，0.0表示最常见)
            self._piece_rarity[distribution_id] = {}
            for piece_id in all_pieces:
                peer_count = len(piece_availability.get(piece_id, []))
                rarity = 1.0 - (peer_count / total_peers if total_peers > 0 else 0)
                self._piece_rarity[distribution_id][piece_id] = rarity
    
    async def _cleanup_stale_peers(self, distribution_id: str, max_inactive_minutes: int = 10):
        """
        清理长时间不活跃的节点
        
        参数:
            distribution_id: 分发任务ID
            max_inactive_minutes: 最大不活跃时间(分钟)
        """
        cutoff_time = datetime.now() - timedelta(minutes=max_inactive_minutes)
        
        # 查询不活跃的节点
        stmt = (
            select(Peer.id, Peer.client_id)
            .where(
                Peer.distribution_id == distribution_id,
                Peer.last_seen < cutoff_time
            )
        )
        result = await self.db_session.execute(stmt)
        stale_peers = result.all()
        
        for peer_id, client_id in stale_peers:
            # 从活动连接中移除
            if client_id in self.active_connections:
                del self.active_connections[client_id]
            
            # 记录事件
            await self._log_event(
                distribution_id=distribution_id,
                peer_id=peer_id,
                event_type="peer_timeout",
                details={"client_id": client_id}
            )
            
            # 将节点标记为已删除
            stmt = (
                delete(Peer)
                .where(Peer.id == peer_id)
            )
            await self.db_session.execute(stmt)
    
    async def register_connection(self, client_id: str, websocket):
        """
        注册新的WebSocket连接
        
        参数:
            client_id: 客户端ID
            websocket: WebSocket连接对象
        """
        self.active_connections[client_id] = websocket
    
    async def unregister_connection(self, client_id: str):
        """
        注销WebSocket连接
        
        参数:
            client_id: 客户端ID
        """
        self.active_connections.pop(client_id, None)
    
    async def create_distribution(self, name: str) -> str:
        """
        创建新的分发任务
        
        参数:
            name: 分发任务名称
            
        返回:
            str: 分发任务ID
        """
        distribution_id = str(uuid.uuid4())
        
        # 创建分发任务记录
        distribution = Distribution(
            id=distribution_id,
            name=name,
            status="active"
        )
        
        self.db_session.add(distribution)
        await self.db_session.commit()
        await self.db_session.flush()
        
        # 添加到内存缓存
        self.distributions[distribution_id] = distribution
        self._piece_availability[distribution_id] = {}
        self._piece_rarity[distribution_id] = {}
        
        # 记录事件
        await self._log_event(
            distribution_id=distribution_id,
            event_type="distribution_created",
            details={"name": name}
        )
        
        return distribution_id

    # 修改 _count_peers 方法为独立操作
    async def _count_peers(self, distribution_id: str) -> int:
        """
        统计分发任务的节点数量（作为独立操作）

        参数:
            distribution_id: 分发任务ID

        返回:
            int: 节点数量
        """

        async def _count(session):
            stmt = select(func.count()).select_from(Peer).where(
                Peer.distribution_id == distribution_id,
                Peer.last_seen > (datetime.utcnow() - timedelta(minutes=5))
            )
            result = await session.execute(stmt)
            return result.scalar() or 0

        return await db_operation(_count)
    # 修改 tracker.py 中的 announce_peer 方法
    async def announce_peer(self, distribution_id: str, ip_address: str, port: int,
                            client_id: str = None, user_agent: str = None, is_seed: bool = False) -> Dict[str, Any]:
        """
        处理节点通告请求

        参数:
            distribution_id: 分发任务ID
            ip_address: 节点IP地址
            port: 节点端口
            client_id: 客户端ID
            user_agent: 用户代理
            is_seed: 是否是种子节点

        返回:
            Dict[str, Any]: 节点信息和分片建议
        """

        async def _announce(session):
            # 检查分发任务是否存在
            stmt = select(Distribution).where(Distribution.id == distribution_id)
            result = await session.execute(stmt)
            distribution = result.scalars().first()

            if not distribution:
                raise ValueError(f"分发任务不存在: {distribution_id}")

            # 查找或创建节点
            stmt = select(Peer).where(
                Peer.distribution_id == distribution_id,
                Peer.client_id == client_id if client_id else Peer.ip_address == ip_address and Peer.port == port
            )
            result = await session.execute(stmt)
            peer = result.scalars().first()

            if not peer:
                peer = Peer(
                    id=str(uuid.uuid4()),
                    distribution_id=distribution_id,
                    ip_address=ip_address,
                    port=port,
                    client_id=client_id,
                    user_agent=user_agent,
                    is_seed=is_seed,
                    last_seen=datetime.utcnow()
                )
                session.add(peer)
            else:
                # 更新节点信息
                peer.last_seen = datetime.utcnow()
                peer.is_seed = is_seed
                if user_agent:
                    peer.user_agent = user_agent

            # 等待此操作完成
            await session.flush()

            # 在同一会话和事务中获取节点信息
            peer_pieces_stmt = select(PeerPiece).where(PeerPiece.peer_id == peer.id)
            peer_pieces_result = await session.execute(peer_pieces_stmt)
            peer_pieces = peer_pieces_result.scalars().all()

            owned_pieces = [pp.piece_id for pp in peer_pieces]

            # 计算活跃节点数量 - 在同一事务中执行
            # 将_count_peers逻辑内联到这里
            count_stmt = select(func.count()).select_from(Peer).where(
                Peer.distribution_id == distribution_id,
                Peer.last_seen > (datetime.utcnow() - timedelta(minutes=5))  # 只计算5分钟内活跃的节点
            )
            count_result = await session.execute(count_stmt)
            peer_count = count_result.scalar() or 0

            # 构造并返回结果
            result = {
                "peer_id": peer.id,
                "distribution_id": distribution_id,
                "owned_pieces": owned_pieces,
                "peer_count": peer_count
            }

            return result

        try:
            # 使用 db_operation 确保事务的正确管理
            return await db_operation(_announce)
        except Exception as e:
            logging.error(f"处理节点通告失败: {e}", exc_info=True)
            raise
    
    async def update_peer_piece(self, peer_id: str, piece_id: str, has_piece: bool = True):
        """
        更新节点的分片状态
        
        参数:
            peer_id: 节点ID
            piece_id: 分片ID
            has_piece: 是否拥有分片
        """
        # 查询节点
        stmt = select(Peer).where(Peer.id == peer_id)
        result = await self.db_session.execute(stmt)
        peer = result.scalars().first()
        
        if not peer:
            raise ValueError(f"节点不存在: {peer_id}")
        
        distribution_id = peer.distribution_id
        
        # 检查分片是否存在
        stmt = select(FilePiece).where(FilePiece.id == piece_id)
        result = await self.db_session.execute(stmt)
        piece = result.scalars().first()
        
        if not piece:
            raise ValueError(f"分片不存在: {piece_id}")
        
        # 查询节点分片记录
        stmt = select(PeerPiece).where(
            PeerPiece.peer_id == peer_id,
            PeerPiece.piece_id == piece_id
        )
        result = await self.db_session.execute(stmt)
        peer_piece = result.scalars().first()
        
        if peer_piece:
            # 更新现有记录
            peer_piece.has_piece = has_piece
            peer_piece.last_updated = datetime.now()
        else:
            # 创建新记录
            peer_piece = PeerPiece(
                id=str(uuid.uuid4()),
                peer_id=peer_id,
                piece_id=piece_id,
                has_piece=has_piece,
                last_updated=datetime.now()
            )
            self.db_session.add(peer_piece)
        
        await self.db_session.flush()
        await self.db_session.commit()
        
        # 更新分片可用性缓存
        if distribution_id in self._piece_availability:
            if piece_id not in self._piece_availability[distribution_id]:
                self._piece_availability[distribution_id][piece_id] = []
            
            if has_piece and peer_id not in self._piece_availability[distribution_id][piece_id]:
                self._piece_availability[distribution_id][piece_id].append(peer_id)
            elif not has_piece and peer_id in self._piece_availability[distribution_id][piece_id]:
                self._piece_availability[distribution_id][piece_id].remove(peer_id)
        
        # 检查节点是否已完成所有分片下载
        await self._check_peer_completed(peer_id, distribution_id)
    
    async def _check_peer_completed(self, peer_id: str, distribution_id: str):
        """
        检查节点是否已完成所有分片下载
        
        参数:
            peer_id: 节点ID
            distribution_id: 分发任务ID
        """
        # 查询分发任务中的所有分片
        stmt = (
            select(func.count())
            .select_from(FilePiece)
            .join(FileInfo, FileInfo.id == FilePiece.file_id)
            .where(FileInfo.distribution_id == distribution_id)
        )
        result = await self.db_session.execute(stmt)
        total_pieces = result.scalar() or 0
        
        # 查询节点已有的分片数量
        stmt = (
            select(func.count())
            .select_from(PeerPiece)
            .where(
                PeerPiece.peer_id == peer_id,
                PeerPiece.has_piece == True
            )
        )
        result = await self.db_session.execute(stmt)
        peer_pieces = result.scalar() or 0
        
        # 如果节点拥有所有分片，标记为做种节点
        if peer_pieces >= total_pieces and total_pieces > 0:
            stmt = (
                update(Peer)
                .where(Peer.id == peer_id)
                .values(is_seed=True)
            )
            await self.db_session.execute(stmt)
            
            # 记录事件
            await self._log_event(
                distribution_id=distribution_id,
                peer_id=peer_id,
                event_type="peer_completed",
                details={"total_pieces": total_pieces}
            )
    
    async def get_peers(self, distribution_id: str, requesting_peer_id: str,
                        max_peers: int = 20) -> List[Dict[str, Any]]:
        """
        获取分发任务中的节点列表
        
        参数:
            distribution_id: 分发任务ID
            requesting_peer_id: 请求的节点ID
            max_peers: 最大返回节点数量
            
        返回:
            List[Dict[str, Any]]: 节点信息列表
        """
        # 查询活跃节点
        stmt = (
            select(Peer)
            .where(
                Peer.distribution_id == distribution_id,
                Peer.id != requesting_peer_id,  # 排除请求节点自身
                Peer.last_seen > datetime.now() - timedelta(minutes=5)  # 仅返回最近活跃的节点
            )
            .order_by(Peer.is_seed.desc(), func.random())  # 做种节点优先，随机排序
            .limit(max_peers)
        )
        result = await self.db_session.execute(stmt)
        peers = result.scalars().all()
        
        return [
            {
                "id": peer.id,
                "client_id": peer.client_id,
                "ip_address": peer.ip_address,
                "port": peer.port,
                "is_seed": peer.is_seed
            }
            for peer in peers
        ]
    
    async def get_piece_suggestions(self, distribution_id: str, peer_id: str, 
                                  max_suggestions: int = 20) -> List[Dict[str, Any]]:
        """
        获取建议下载的分片列表
        
        参数:
            distribution_id: 分发任务ID
            peer_id: 节点ID
            max_suggestions: 最大建议数量
            
        返回:
            List[Dict[str, Any]]: 建议下载的分片信息列表
        """
        print("peer_id=",peer_id,";distribution_id=",distribution_id)
        # 查询节点已拥有的分片
        stmt = (
            select(PeerPiece.piece_id)
            .where(
                PeerPiece.peer_id == peer_id,
                PeerPiece.has_piece == True
            )
        )
        result = await self.db_session.execute(stmt)
        owned_pieces = {piece_id for piece_id, in result}
        
        # 查询分发任务中的所有分片
        stmt = (
            select(FilePiece.id, FilePiece.file_id, FilePiece.index,
                  FileInfo.priority)
            .join(FileInfo, FileInfo.id == FilePiece.file_id)
            .where(FileInfo.distribution_id == distribution_id)
            .order_by(FileInfo.priority.desc(), FilePiece.index)
        )
        result = await self.db_session.execute(stmt)
        all_pieces = result.all()
        
        # 过滤掉已拥有的分片
        missing_pieces = [
            (piece_id, file_id, index, priority)
            for piece_id, file_id, index, priority in all_pieces
            if piece_id not in owned_pieces
        ]
        
        # 如果没有缺失的分片，返回空列表
        if not missing_pieces:
            return []
        
        # 计算每个缺失分片的稀有度分数
        piece_scores = []
        for piece_id, file_id, index, priority in missing_pieces:
            # 基于稀有度、优先级和文件内位置计算分数
            rarity_score = self._piece_rarity.get(distribution_id, {}).get(piece_id, 1.0)
            position_score = 1.0 - (index / len(all_pieces)) if all_pieces else 0.5
            
            # 稀有度权重0.4，优先级权重0.4，位置权重0.2
            final_score = (0.4 * rarity_score) + (0.4 * (priority / 10.0)) + (0.2 * position_score)
            piece_scores.append((piece_id, file_id, final_score))
        
        # 按分数排序，取前N个
        piece_scores.sort(key=lambda x: x[2], reverse=True)
        suggestions = piece_scores[:max_suggestions]
        
        # 查询可下载的节点信息
        result = []
        for piece_id, file_id, _ in suggestions:
            available_peers = self._piece_availability.get(distribution_id, {}).get(piece_id, [])
            
            if not available_peers:
                continue
            
            # 查询文件和分片信息
            stmt = (
                select(FilePiece, FileInfo)
                .join(FileInfo, FileInfo.id == FilePiece.file_id)
                .where(FilePiece.id == piece_id)
            )
            query_result = await self.db_session.execute(stmt)
            piece_info = query_result.first()
            
            if not piece_info:
                continue
            
            piece, file_info = piece_info
            
            # 查询可用节点信息
            stmt = (
                select(Peer)
                .where(
                    Peer.id.in_(available_peers),
                    Peer.last_seen > datetime.now() - timedelta(minutes=5)
                )
                .limit(5)  # 每个分片最多返回5个可用节点
            )
            query_result = await self.db_session.execute(stmt)
            available_peer_objects = query_result.scalars().all()
            
            result.append({
                "piece_id": piece_id,
                "file_id": file_id,
                "file_path": file_info.path,
                "piece_index": piece.index,
                "piece_size": piece.size,
                "available_peers": [
                    {
                        "peer_id": p.id,
                        "ip_address": p.ip_address,
                        "port": p.port
                    } 
                    for p in available_peer_objects
                ]
            })
        
        return result
    
    async def get_distribution_status(self, distribution_id: str) -> Dict[str, Any]:
        """
        获取分发任务状态
        
        参数:
            distribution_id: 分发任务ID
            
        返回:
            Dict[str, Any]: 分发任务状态信息
        """
        # 查询分发任务信息
        stmt = select(Distribution).where(Distribution.id == distribution_id)
        result = await self.db_session.execute(stmt)
        distribution = result.scalars().first()
        
        if not distribution:
            raise ValueError(f"分发任务不存在: {distribution_id}")
        
        # 查询所有文件和分片数量
        stmt = (
            select(FileInfo.id, FileInfo.path, FileInfo.size, func.count(FilePiece.id).label('piece_count'))
            .join(FilePiece, FilePiece.file_id == FileInfo.id)
            .where(FileInfo.distribution_id == distribution_id)
            .group_by(FileInfo.id)
        )
        result = await self.db_session.execute(stmt)
        files = result.all()
        
        # 查询活跃节点数量和完成节点数量
        stmt = (
            select(
                func.count().label('active_peers'),
                func.sum(Peer.is_seed.cast(Integer)).label('completed_peers')
            )
            .where(
                Peer.distribution_id == distribution_id,
                Peer.last_seen > datetime.now() - timedelta(minutes=5)
            )
        )
        result = await self.db_session.execute(stmt)
        peer_stats = result.first()
        
        active_peers = peer_stats.active_peers if peer_stats else 0
        completed_peers = peer_stats.completed_peers if peer_stats else 0
        
        # 计算整体完成度
        if active_peers > 0:
            # 查询每个节点的完成度
            stmt = (
                select(Peer.id, func.count().label('has_pieces'))
                .join(PeerPiece, PeerPiece.peer_id == Peer.id)
                .where(
                    Peer.distribution_id == distribution_id,
                    Peer.last_seen > datetime.now() - timedelta(minutes=5),
                    PeerPiece.has_piece == True
                )
                .group_by(Peer.id)
            )
            result = await self.db_session.execute(stmt)
            peer_pieces = {peer_id: has_pieces for peer_id, has_pieces in result}
            
            # 计算总分片数量
            total_pieces = sum(piece_count for _, _, _, piece_count in files)
            
            if total_pieces > 0:
                # 计算每个节点的完成百分比
                peer_completion = [
                    min(1.0, has_pieces / total_pieces) if total_pieces > 0 else 0.0
                    for has_pieces in peer_pieces.values()
                ]
                
                # 整体完成度是所有节点完成度的平均值
                overall_completion = sum(peer_completion) / len(peer_completion) if peer_completion else 0.0
            else:
                overall_completion = 0.0
        else:
            overall_completion = 0.0
        
        return {
            "id": distribution.id,
            "name": distribution.name,
            "created_at": distribution.created_at.isoformat(),
            "status": distribution.status,
            "files": [
                {
                    "id": file_id,
                    "path": path,
                    "size": size,
                    "piece_count": piece_count
                }
                for file_id, path, size, piece_count in files
            ],
            "stats": {
                "active_peers": active_peers,
                "completed_peers": completed_peers,
                "overall_completion": overall_completion
            }
        }
    
    async def _log_event(self, event_type: str, distribution_id: str = None, 
                        peer_id: str = None, details: Dict[str, Any] = None):
        """
        记录追踪器事件
        
        参数:
            event_type: 事件类型
            distribution_id: 分发任务ID
            peer_id: 节点ID
            details: 详细信息
        """
        event = TrackerEvent(
            distribution_id=distribution_id,
            peer_id=peer_id,
            event_type=event_type,
            timestamp=datetime.now()
        )
        
        if details:
            event.set_details(details)
        
        self.db_session.add(event)
        await self.db_session.flush()
        await self.db_session.flush()