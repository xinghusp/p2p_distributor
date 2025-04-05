"""P2P Tracker核心逻辑"""
import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple

from sqlalchemy import select, update, func, Integer, delete, or_, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from database import Distribution, FileInfo, FilePiece, Peer, PeerPiece, TrackerEvent, db_operation


class P2PTracker:
    """P2P追踪器实现"""

    def __init__(self):
        """初始化追踪器 - 不再接受和存储db_session"""
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
        async def _load(session):
            stmt = select(Distribution).where(Distribution.status == "active")
            result = await session.execute(stmt)
            return result.scalars().all()

        active_distributions = await db_operation(_load)

        for dist in active_distributions:
            self.distributions[dist.id] = dist
            # 初始化分片可用性缓存
            await self._initialize_piece_availability(dist.id)

    async def _initialize_piece_availability(self, distribution_id: str):
        """初始化分片可用性缓存"""
        self._piece_availability[distribution_id] = {}
        self._piece_rarity[distribution_id] = {}

        async def _init_availability(session):
            # 查询所有拥有分片的节点
            stmt = (
                select(PeerPiece.piece_id, PeerPiece.peer_id)
                .join(Peer, Peer.id == PeerPiece.peer_id)
                .where(
                    Peer.distribution_id == distribution_id,
                    PeerPiece.has_piece == True
                )
            )
            result = await session.execute(stmt)
            return result.all()

        peer_pieces = await db_operation(_init_availability)

        # 构建分片可用性缓存
        for piece_id, peer_id in peer_pieces:
            if piece_id not in self._piece_availability[distribution_id]:
                self._piece_availability[distribution_id][piece_id] = []
            self._piece_availability[distribution_id][piece_id].append(peer_id)

        # 计算初始稀有度
        await self._update_piece_rarity(distribution_id)

    async def _refresh_cache_periodically(self, interval: int = 30):
        """定期刷新缓存"""
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
        """定期向客户端发送心跳"""
        while True:
            try:
                await asyncio.sleep(interval)
                timestamp = datetime.now().isoformat()

                # 向所有活动连接发送心跳
                for client_id, connection in list(self.active_connections.items()):
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
        """更新分片可用性缓存"""
        async def _update(session):
            # 查询所有拥有分片的节点
            stmt = (
                select(PeerPiece.piece_id, PeerPiece.peer_id)
                .join(Peer, Peer.id == PeerPiece.peer_id)
                .where(
                    Peer.distribution_id == distribution_id,
                    PeerPiece.has_piece == True,
                    Peer.last_seen > datetime.now() - timedelta(minutes=5)
                )
            )
            result = await session.execute(stmt)
            return result.all()

        peer_pieces = await db_operation(_update)

        # 重建分片可用性缓存
        self._piece_availability[distribution_id] = {}
        for piece_id, peer_id in peer_pieces:
            if piece_id not in self._piece_availability[distribution_id]:
                self._piece_availability[distribution_id][piece_id] = []
            self._piece_availability[distribution_id][piece_id].append(peer_id)

    async def _update_piece_rarity(self, distribution_id: str):
        """更新分片稀有度缓存"""
        if distribution_id not in self._piece_availability:
            return

        async def _get_pieces(session):
            # 查询该分发任务的所有分片
            stmt = (
                select(FilePiece.id)
                .join(FileInfo, FileInfo.id == FilePiece.file_id)
                .where(FileInfo.distribution_id == distribution_id)
            )
            result = await session.execute(stmt)
            return result.scalars().all()

        all_pieces = await db_operation(_get_pieces)

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
        """清理长时间不活跃的节点"""
        cutoff_time = datetime.now() - timedelta(minutes=max_inactive_minutes)

        async def _cleanup(session):
            # 查询不活跃的节点
            stmt = (
                select(Peer.id, Peer.client_id)
                .where(
                    Peer.distribution_id == distribution_id,
                    Peer.last_seen < cutoff_time,
                    Peer.is_seed == False  # 不删除种子节点
                )
            )
            result = await session.execute(stmt)
            stale_peers = result.all()

            for peer_id, client_id in stale_peers:
                # 记录事件
                event = TrackerEvent(
                    distribution_id=distribution_id,
                    peer_id=peer_id,
                    event_type="peer_timeout",
                    timestamp=datetime.now()
                )
                if client_id:
                    event.set_details({"client_id": client_id})
                session.add(event)

                # 删除节点
                stmt = delete(Peer).where(Peer.id == peer_id)
                await session.execute(stmt)

            return stale_peers

        stale_peers = await db_operation(_cleanup)

        # 从活动连接中移除
        for _, client_id in stale_peers:
            if client_id in self.active_connections:
                self.active_connections.pop(client_id, None)

    async def register_connection(self, client_id: str, websocket):
        """注册新的WebSocket连接"""
        self.active_connections[client_id] = websocket

    async def unregister_connection(self, client_id: str):
        """注销WebSocket连接"""
        self.active_connections.pop(client_id, None)

    async def create_distribution(self, name: str) -> str:
        """创建新的分发任务"""
        distribution_id = str(uuid.uuid4())

        async def _create(session):
            # 创建分发任务记录
            distribution = Distribution(
                id=distribution_id,
                name=name,
                status="active"
            )

            session.add(distribution)

            # 创建事件记录
            event = TrackerEvent(
                distribution_id=distribution_id,
                event_type="distribution_created",
                timestamp=datetime.now()
            )
            event.set_details({"name": name})
            session.add(event)

            return distribution

        distribution = await db_operation(_create)

        # 添加到内存缓存
        self.distributions[distribution_id] = distribution
        self._piece_availability[distribution_id] = {}
        self._piece_rarity[distribution_id] = {}

        return distribution_id

    async def announce_peer(self, distribution_id: str, ip_address: str, port: int,
                            client_id: str = None, user_agent: str = None, is_seed: bool = False) -> Dict[str, Any]:
        """
        处理节点通告请求
        """
        try:
            logging.info(
                f"开始处理节点通告: distribution_id={distribution_id}, client_id={client_id}, ip={ip_address}:{port}")

            # 首先确认分发任务是否存在
            async def check_distribution(session):
                stmt = select(Distribution).where(Distribution.id == distribution_id)
                result = await session.execute(stmt)
                return result.scalars().first()

            distribution = await db_operation(check_distribution)

            if not distribution:
                logging.error(f"分发任务不存在: {distribution_id}")
                raise ValueError(f"分发任务不存在: {distribution_id}")

            # 创建或更新节点 - 使用独立的简单方法，降低复杂性
            peer_id = None

            # 简化查找现有节点的逻辑
            async def find_peer(session):
                search_conditions = []
                if client_id:
                    search_conditions.append(Peer.client_id == client_id)
                else:
                    search_conditions.append(Peer.ip_address == ip_address)
                    search_conditions.append(Peer.port == port)

                stmt = select(Peer).where(
                    Peer.distribution_id == distribution_id,
                    *search_conditions
                )
                result = await session.execute(stmt)
                return result.scalars().first()

            existing_peer = await db_operation(find_peer)

            if existing_peer:
                # 更新现有节点
                async def update_existing_peer(session):
                    peer = await session.get(Peer, existing_peer.id)
                    if peer:
                        peer.last_seen = datetime.utcnow()
                        peer.ip_address = ip_address
                        peer.port = port
                        peer.is_seed = is_seed
                        if user_agent:
                            peer.user_agent = user_agent
                        await session.flush()  # 确保更改被保存
                        return peer
                    return None

                peer = await db_operation(update_existing_peer)
                if peer:
                    peer_id = peer.id
                    logging.info(
                        f"更新现有节点: id={peer_id}, distribution_id={distribution_id}, client_id={client_id}")
            else:
                # 创建新节点
                async def create_new_peer(session):
                    new_peer_id = str(uuid.uuid4())
                    peer = Peer(
                        id=new_peer_id,
                        distribution_id=distribution_id,
                        ip_address=ip_address,
                        port=port,
                        client_id=client_id,
                        user_agent=user_agent,
                        is_seed=is_seed,
                        last_seen=datetime.utcnow()
                    )
                    session.add(peer)
                    await session.flush()  # 确保ID被生成并可用
                    return peer

                peer = await db_operation(create_new_peer)
                peer_id = peer.id
                logging.info(
                    f"创建新节点: id={peer_id}, distribution_id={distribution_id}, client_id={client_id}, ip={ip_address}:{port}")

            # 特别确认创建/更新成功 - 强制直接访问数据库进行验证
            async def verify_peer(session):
                stmt = text("""
                    SELECT id, client_id, distribution_id FROM peers
                    WHERE id = :peer_id
                """)
                result = await session.execute(stmt, {"peer_id": peer_id})
                row = result.first()
                if row:
                    logging.info(f"验证节点成功存在: id={row[0]}, client_id={row[1]}, distribution_id={row[2]}")
                    return True
                else:
                    logging.error(f"验证失败，节点不存在: id={peer_id}")
                    return False

            verified = await db_operation(verify_peer)
            if not verified:
                # 如果验证失败，尝试最后一次强制创建
                async def force_create_peer(session):
                    # 使用原始SQL插入以避免任何ORM问题
                    stmt = text("""
                        INSERT INTO peers (id, distribution_id, client_id, ip_address, port, user_agent, is_seed, joined_at, last_seen)
                        VALUES (:id, :distribution_id, :client_id, :ip_address, :port, :user_agent, :is_seed, :now, :now)
                    """)
                    await session.execute(stmt, {
                        "id": peer_id or str(uuid.uuid4()),
                        "distribution_id": distribution_id,
                        "client_id": client_id,
                        "ip_address": ip_address,
                        "port": port,
                        "user_agent": user_agent or "",
                        "is_seed": is_seed,
                        "now": datetime.utcnow()
                    })
                    # 强制提交
                    await session.commit()
                    logging.info("强制创建节点并提交")
                    return peer_id

                peer_id = await db_operation(force_create_peer)

            # 获取节点拥有的分片列表
            async def get_peer_pieces(session):
                stmt = select(PeerPiece).where(PeerPiece.peer_id == peer_id)
                result = await session.execute(stmt)
                return [pp.piece_id for pp in result.scalars().all()]

            owned_pieces = await db_operation(get_peer_pieces)

            # 计算活跃节点数量
            async def count_peers(session):
                stmt = select(func.count()).select_from(Peer).where(
                    Peer.distribution_id == distribution_id,
                    Peer.last_seen > (datetime.utcnow() - timedelta(minutes=5))
                )
                result = await session.execute(stmt)
                return result.scalar() or 0

            active_peers = await db_operation(count_peers)

            # 构造并返回结果
            result = {
                "peer_id": peer_id,
                "distribution_id": distribution_id,
                "owned_pieces": owned_pieces,
                "peer_count": active_peers
            }

            logging.info(
                f"节点通告处理成功: peer_id={peer_id}, distribution_id={distribution_id}, owned_pieces={len(owned_pieces)}")
            return result

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
        try:
            logging.info(f"开始更新节点分片状态: peer_id={peer_id}, piece_id={piece_id}, has_piece={has_piece}")

            # 查询节点 - 使用db_operation
            async def check_peer(session):
                stmt = select(Peer).where(Peer.id == peer_id)
                result = await session.execute(stmt)
                return result.scalars().first()

            peer = await db_operation(check_peer)

            if not peer:
                logging.error(f"节点不存在: {peer_id}")
                raise ValueError(f"节点不存在: {peer_id}")

            distribution_id = peer.distribution_id

            # 检查分片是否存在 - 使用db_operation
            async def check_piece(session):
                stmt = select(FilePiece).where(FilePiece.id == piece_id)
                result = await session.execute(stmt)
                return result.scalars().first()

            piece = await db_operation(check_piece)

            if not piece:
                logging.error(f"分片不存在: {piece_id}")
                raise ValueError(f"分片不存在: {piece_id}")

            # 更新或创建节点分片记录 - 使用db_operation
            async def update_piece(session):
                # 查询节点分片记录
                stmt = select(PeerPiece).where(
                    PeerPiece.peer_id == peer_id,
                    PeerPiece.piece_id == piece_id
                )
                result = await session.execute(stmt)
                peer_piece = result.scalars().first()

                if peer_piece:
                    # 更新现有记录
                    peer_piece.has_piece = has_piece
                    peer_piece.last_updated = datetime.now()
                    logging.info(f"更新现有分片记录: peer_id={peer_id}, piece_id={piece_id}")
                else:
                    # 创建新记录
                    peer_piece = PeerPiece(
                        id=str(uuid.uuid4()),
                        peer_id=peer_id,
                        piece_id=piece_id,
                        has_piece=has_piece,
                        last_updated=datetime.now()
                    )
                    session.add(peer_piece)
                    logging.info(f"创建新分片记录: peer_id={peer_id}, piece_id={piece_id}")

                # 确保记录会被提交
                return True

            await db_operation(update_piece)

            # 更新分片可用性缓存
            if distribution_id in self._piece_availability:
                if piece_id not in self._piece_availability[distribution_id]:
                    self._piece_availability[distribution_id][piece_id] = []

                if has_piece and peer_id not in self._piece_availability[distribution_id][piece_id]:
                    self._piece_availability[distribution_id][piece_id].append(peer_id)
                    logging.debug(f"更新分片可用性缓存: 添加 peer_id={peer_id}, piece_id={piece_id}")
                elif not has_piece and peer_id in self._piece_availability[distribution_id][piece_id]:
                    self._piece_availability[distribution_id][piece_id].remove(peer_id)
                    logging.debug(f"更新分片可用性缓存: 移除 peer_id={peer_id}, piece_id={piece_id}")

            # 检查节点是否已完成所有分片下载 - 在单独事务中执行
            await self._check_peer_completed(peer_id, distribution_id)
            logging.info(f"节点分片状态更新成功: peer_id={peer_id}, piece_id={piece_id}")

        except Exception as e:
            logging.error(f"更新节点分片状态失败: {e}", exc_info=True)
            raise

    async def _check_peer_completed(self, peer_id: str, distribution_id: str):
        """检查节点是否已完成所有分片下载"""
        async def _check(session):
            # 查询分发任务中的所有分片
            stmt = (
                select(func.count())
                .select_from(FilePiece)
                .join(FileInfo, FileInfo.id == FilePiece.file_id)
                .where(FileInfo.distribution_id == distribution_id)
            )
            result = await session.execute(stmt)
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
            result = await session.execute(stmt)
            peer_pieces = result.scalar() or 0

            # 如果节点拥有所有分片，标记为做种节点
            if peer_pieces >= total_pieces and total_pieces > 0:
                stmt = (
                    update(Peer)
                    .where(Peer.id == peer_id)
                    .values(is_seed=True)
                )
                await session.execute(stmt)

                # 记录事件
                event = TrackerEvent(
                    distribution_id=distribution_id,
                    peer_id=peer_id,
                    event_type="peer_completed",
                    timestamp=datetime.now()
                )
                event.set_details({"total_pieces": total_pieces})
                session.add(event)

                return True
            return False

        return await db_operation(_check)

    async def get_peers(self, distribution_id: str, requesting_peer_id: str = None,
                        max_peers: int = 20) -> List[Dict[str, Any]]:
        """获取分发任务中的节点列表"""
        async def _get_peers(session):
            # 设置较宽松的活跃时间阈值
            active_threshold = datetime.utcnow() - timedelta(minutes=30)

            # 构建查询
            stmt = select(Peer).where(
                Peer.distribution_id == distribution_id,
                or_(
                    Peer.is_seed == True,  # 种子节点总是包含
                    Peer.last_seen > active_threshold  # 或者是最近活跃的节点
                )
            )

            # 如果提供了请求节点ID，排除自身
            if requesting_peer_id:
                stmt = stmt.where(Peer.id != requesting_peer_id)

            # 做种节点优先，然后按最近活跃时间排序
            stmt = stmt.order_by(Peer.is_seed.desc(), Peer.last_seen.desc())

            # 限制返回数量
            if max_peers > 0:
                stmt = stmt.limit(max_peers)

            result = await session.execute(stmt)
            peers = result.scalars().all()

            # 添加调试日志
            logging.info(f"查询到节点数量: {len(peers)} 个，分发任务: {distribution_id}")
            for i, peer in enumerate(peers):
                logging.debug(f"节点{i+1}: ID={peer.id}, 种子={peer.is_seed}, IP={peer.ip_address}, 上次活跃={peer.last_seen}")

            # 转换为字典列表返回
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

        return await db_operation(_get_peers)

    async def get_piece_suggestions(self, distribution_id: str, peer_id: str,
                                  max_suggestions: int = 20) -> List[Dict[str, Any]]:
        """获取建议下载的分片列表"""
        logging.debug(f"获取分片建议: peer_id={peer_id}, distribution_id={distribution_id}")

        async def _get_suggestions(session):
            # 查询节点已拥有的分片
            stmt = (
                select(PeerPiece.piece_id)
                .where(
                    PeerPiece.peer_id == peer_id,
                    PeerPiece.has_piece == True
                )
            )
            result = await session.execute(stmt)
            owned_pieces = {piece_id for piece_id, in result}

            # 查询分发任务中的所有分片
            stmt = (
                select(FilePiece.id, FilePiece.file_id, FilePiece.index, FilePiece.size,
                      FileInfo.priority, FileInfo.path)
                .join(FileInfo, FileInfo.id == FilePiece.file_id)
                .where(FileInfo.distribution_id == distribution_id)
                .order_by(FileInfo.priority.desc(), FilePiece.index)
            )
            result = await session.execute(stmt)
            all_pieces = result.all()

            # 过滤掉已拥有的分片
            missing_pieces = [
                (piece_id, file_id, index, size, priority, path)
                for piece_id, file_id, index, size, priority, path in all_pieces
                if piece_id not in owned_pieces
            ]

            # 如果没有缺失的分片，返回空列表
            if not missing_pieces:
                return []

            # 计算每个缺失分片的稀有度分数
            piece_scores = []
            for piece_id, file_id, index, size, priority, path in missing_pieces:
                # 基于稀有度、优先级和文件内位置计算分数
                rarity_score = self._piece_rarity.get(distribution_id, {}).get(piece_id, 1.0)
                position_score = 1.0 - (index / len(all_pieces)) if all_pieces else 0.5

                # 稀有度权重0.4，优先级权重0.4，位置权重0.2
                final_score = (0.4 * rarity_score) + (0.4 * (priority / 10.0)) + (0.2 * position_score)
                piece_scores.append((piece_id, file_id, index, size, path, final_score))

            # 按分数排序，取前N个
            piece_scores.sort(key=lambda x: x[5], reverse=True)
            suggestions = piece_scores[:max_suggestions]

            # 构建结果
            result = []
            for piece_id, file_id, index, size, path, _ in suggestions:
                # 查询拥有该分片的节点
                available_peers = []
                if distribution_id in self._piece_availability and piece_id in self._piece_availability[distribution_id]:
                    # 从缓存中获取
                    available_peer_ids = self._piece_availability[distribution_id][piece_id]

                    # 查询这些节点的详细信息
                    if available_peer_ids:
                        stmt = (
                            select(Peer)
                            .where(
                                Peer.id.in_(available_peer_ids),
                                Peer.last_seen > datetime.utcnow() - timedelta(minutes=5)
                            )
                            .limit(5)  # 每个分片最多返回5个可用节点
                        )
                        peers_result = await session.execute(stmt)
                        available_peers = [
                            {
                                "peer_id": p.id,
                                "ip_address": p.ip_address,
                                "port": p.port
                            }
                            for p in peers_result.scalars()
                        ]

                # 如果没有可用节点但这是种子节点查询，查找种子节点
                if not available_peers:
                    stmt = (
                        select(Peer)
                        .where(
                            Peer.distribution_id == distribution_id,
                            Peer.is_seed == True
                        )
                        .limit(5)
                    )
                    seeds_result = await session.execute(stmt)
                    available_peers = [
                        {
                            "peer_id": p.id,
                            "ip_address": p.ip_address,
                            "port": p.port,
                            "is_seed": True
                        }
                        for p in seeds_result.scalars()
                    ]

                if available_peers:  # 只返回有可用节点的分片
                    result.append({
                        "piece_id": piece_id,
                        "file_id": file_id,
                        "file_path": path,
                        "piece_index": index,
                        "piece_size": size,
                        "available_peers": available_peers
                    })

            return result

        return await db_operation(_get_suggestions)

    async def get_distribution_status(self, distribution_id: str) -> Dict[str, Any]:
        """获取分发任务状态"""
        async def _get_status(session):
            # 查询分发任务信息
            stmt = select(Distribution).where(Distribution.id == distribution_id)
            result = await session.execute(stmt)
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
            result = await session.execute(stmt)
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
            result = await session.execute(stmt)
            peer_stats = result.first()

            active_peers = peer_stats.active_peers if peer_stats else 0
            completed_peers = peer_stats.completed_peers if peer_stats else 0

            # 计算整体完成度
            overall_completion = 0.0

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
                result = await session.execute(stmt)
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
                    if peer_completion:
                        overall_completion = sum(peer_completion) / len(peer_completion)

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

        return await db_operation(_get_status)

    async def _log_event(self, event_type: str, distribution_id: str = None,
                       peer_id: str = None, details: Dict[str, Any] = None):
        """记录追踪器事件"""
        async def _log(session):
            event = TrackerEvent(
                distribution_id=distribution_id,
                peer_id=peer_id,
                event_type=event_type,
                timestamp=datetime.now()
            )

            if details:
                event.set_details(details)

            session.add(event)

        await db_operation(_log)