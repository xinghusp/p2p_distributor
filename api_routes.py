"""API路由定义"""
import os
import logging
import uuid
from typing import Dict, List, Optional, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, WebSocket
from fastapi.responses import JSONResponse
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import Distribution, FileInfo, Peer, FilePiece, db_operation
from file_processor import FileProcessor
from tracker import P2PTracker
from websocket_handler import websocket_endpoint, manager

import time
from functools import lru_cache, wraps

# 创建路由器
router = APIRouter()

# 全局P2P追踪器实例
tracker_instance = None

# 缓存变量
_distributions_cache = None
_distributions_cache_time = 0
_CACHE_TTL = 5  # 秒

def async_cached():
    """为异步函数提供的简单缓存装饰器"""
    cache = {}

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 创建一个简单的缓存键
            key = str(args) + str(kwargs)

            # 检查缓存是否有效
            now = time.time()
            if key in cache and cache[key]['expires'] > now:
                return cache[key]['data']

            # 执行函数并缓存结果
            result = await func(*args, **kwargs)
            cache[key] = {
                'data': result,
                'expires': now + _CACHE_TTL
            }

            return result

        # 添加清除缓存的方法
        def clear_cache():
            cache.clear()

        wrapper.clear_cache = clear_cache
        return wrapper

    return decorator

def get_tracker() -> P2PTracker:
    """
    获取P2P追踪器实例

    返回:
        P2PTracker: P2P追踪器实例
    """
    global tracker_instance
    if tracker_instance is None:
        tracker_instance = P2PTracker()
    return tracker_instance

@router.post("/api/distributions", response_model=Dict[str, Any])
async def create_distribution(
    name: str = Form(...),
    tracker: P2PTracker = Depends(get_tracker)
):
    """
    创建新的分发任务

    参数:
        name: 分发任务名称
        tracker: P2P追踪器

    返回:
        Dict[str, Any]: 包含分发任务ID的字典
    """
    try:
        distribution_id = await tracker.create_distribution(name)

        return {
            "status": "success",
            "distribution_id": distribution_id,
            "message": "分发任务已创建"
        }
    except Exception as e:
        logging.error(f"创建分发任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"创建分发任务失败: {str(e)}")

@router.post("/api/distributions/{distribution_id}/files", response_model=Dict[str, Any])
async def upload_file(
        distribution_id: str,
        file: UploadFile = File(...),
        path: Optional[str] = Form(None),
        priority: int = Form(0)
):
    """上传文件到分发任务"""
    temp_file_path = None
    try:
        # 检查分发任务是否存在
        async def check_distribution(session):
            stmt = select(Distribution).where(Distribution.id == distribution_id)
            result = await session.execute(stmt)
            return result.scalars().first()

        distribution = await db_operation(check_distribution)

        if not distribution:
            raise HTTPException(status_code=404, detail=f"分发任务不存在: {distribution_id}")

        # 使用上传的文件名如果没有提供路径
        file_path = path if path else file.filename

        # 保存上传的文件到临时目录
        temp_file_path = f"temp/{uuid.uuid4()}_{file.filename}"
        os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)

        with open(temp_file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)

        # 处理文件 - 注意不再传递数据库会话
        file_processor = FileProcessor()
        file_info = await file_processor.process_file(
            file_path=temp_file_path,
            file_name=file_path,
            distribution_id=distribution_id,
            priority=priority
        )

        # 更新分发任务总大小
        async def update_distribution_size(session):
            dist = await session.get(Distribution, distribution_id)
            if dist:
                dist.total_size += file_info.size
            return dist

        await db_operation(update_distribution_size)

        # 清除缓存（如果有）
        clear_distributions_cache()

        return {
            "status": "success",
            "file_id": file_info.id,
            "message": "文件已上传并处理"
        }

    except Exception as e:
        logging.error(f"上传文件失败: {e}", exc_info=True)

        if "database is locked" in str(e):
            raise HTTPException(status_code=503, detail="数据库暂时繁忙，请稍后重试")
        else:
            raise HTTPException(status_code=500, detail=f"上传文件失败: {str(e)}")
    finally:
        # 清理临时文件
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                logging.warning(f"清理临时文件失败: {e}")

async def get_cached_distributions():
    """获取带缓存的分发任务列表"""
    global _distributions_cache, _distributions_cache_time

    now = time.time()
    if _distributions_cache and (now - _distributions_cache_time) < _CACHE_TTL:
        return _distributions_cache

    async def fetch_distributions(session):
        # 查询所有分发任务
        stmt = select(Distribution).order_by(Distribution.created_at.desc())
        result = await session.execute(stmt)
        distributions = result.scalars().all()

        # 转换为字典列表
        dist_dicts = []
        for dist in distributions:
            dist_dict = {
                "id": dist.id,
                "name": dist.name,
                "created_at": dist.created_at.isoformat() if dist.created_at else None,
                "status": dist.status,
                "total_size": dist.total_size,
                "files": [],
                "peer_count": 0
            }

            # 为每个分发任务查询文件信息
            files_stmt = select(FileInfo).where(FileInfo.distribution_id == dist.id)
            files_result = await session.execute(files_stmt)
            files = files_result.scalars().all()

            # 处理文件信息
            for file in files:
                # 查询分片数量
                piece_count_stmt = select(func.count()).select_from(FilePiece).where(FilePiece.file_id == file.id)
                piece_count_result = await session.execute(piece_count_stmt)
                piece_count = piece_count_result.scalar() or 0

                # 构建文件字典
                file_dict = {
                    "id": file.id,
                    "path": file.path,
                    "size": file.size,
                    "priority": file.priority,
                    "sha256": file.sha256,
                    "piece_count": piece_count
                }
                dist_dict["files"].append(file_dict)

            # 查询活跃节点数量
            peers_stmt = select(func.count()).select_from(Peer).where(Peer.distribution_id == dist.id)
            peers_result = await session.execute(peers_stmt)
            dist_dict["peer_count"] = peers_result.scalar() or 0

            dist_dicts.append(dist_dict)

        return dist_dicts

    # 使用db_operation获取数据
    dist_dicts = await db_operation(fetch_distributions)

    # 更新缓存
    _distributions_cache = dist_dicts
    _distributions_cache_time = now

    return dist_dicts

# 清除缓存的函数
def clear_distributions_cache():
    global _distributions_cache, _distributions_cache_time
    _distributions_cache = None
    _distributions_cache_time = 0

@router.get("/api/distributions", response_model=List[Dict[str, Any]])
async def list_distributions():
    """
    获取所有分发任务列表

    返回:
        List[Dict[str, Any]]: 分发任务列表
    """
    try:
        return await get_cached_distributions()
    except Exception as e:
        logging.error(f"获取分发任务列表失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取分发任务列表失败: {str(e)}")

@router.get("/api/distributions/{distribution_id}", response_model=Dict[str, Any])
async def get_distribution(
    distribution_id: str,
    tracker: P2PTracker = Depends(get_tracker)
):
    """
    获取分发任务详情

    参数:
        distribution_id: 分发任务ID
        tracker: P2P追踪器

    返回:
        Dict[str, Any]: 分发任务详情
    """
    try:
        distribution_status = await tracker.get_distribution_status(distribution_id)
        return distribution_status
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logging.error(f"获取分发任务详情失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取分发任务详情失败: {str(e)}")

@router.post("/api/distributions/{distribution_id}/start", response_model=Dict[str, Any])
async def start_distribution(distribution_id: str):
    """
    启动分发任务

    参数:
        distribution_id: 分发任务ID

    返回:
        Dict[str, Any]: 操作结果
    """
    try:
        async def update_status(session):
            # 检查分发任务是否存在
            stmt = select(Distribution).where(Distribution.id == distribution_id)
            result = await session.execute(stmt)
            distribution = result.scalars().first()

            if not distribution:
                raise ValueError(f"分发任务不存在: {distribution_id}")

            # 更新状态为活动
            distribution.status = "active"
            return distribution

        distribution = await db_operation(update_status)

        # 广播状态更新
        await manager.broadcast_to_distribution(distribution_id, {
            "type": "distribution_status_update",
            "distribution_id": distribution_id,
            "status": "active"
        })

        # 清除缓存
        clear_distributions_cache()

        return {
            "status": "success",
            "message": "分发任务已启动"
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logging.error(f"启动分发任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"启动分发任务失败: {str(e)}")

@router.post("/api/distributions/{distribution_id}/pause", response_model=Dict[str, Any])
async def pause_distribution(distribution_id: str):
    """
    暂停分发任务

    参数:
        distribution_id: 分发任务ID

    返回:
        Dict[str, Any]: 操作结果
    """
    try:
        async def update_status(session):
            # 检查分发任务是否存在
            stmt = select(Distribution).where(Distribution.id == distribution_id)
            result = await session.execute(stmt)
            distribution = result.scalars().first()

            if not distribution:
                raise ValueError(f"分发任务不存在: {distribution_id}")

            # 更新状态为暂停
            distribution.status = "paused"
            return distribution

        distribution = await db_operation(update_status)

        # 广播状态更新
        await manager.broadcast_to_distribution(distribution_id, {
            "type": "distribution_status_update",
            "distribution_id": distribution_id,
            "status": "paused"
        })

        # 清除缓存
        clear_distributions_cache()

        return {
            "status": "success",
            "message": "分发任务已暂停"
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logging.error(f"暂停分发任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"暂停分发任务失败: {str(e)}")

@router.websocket("/ws")
async def websocket_route(websocket: WebSocket):
    """
    WebSocket路由

    参数:
        websocket: WebSocket连接
    """
    await websocket_endpoint(websocket)

@router.websocket("/ws/{client_id}")
async def websocket_route_with_client_id(websocket: WebSocket, client_id: str):
    """
    带客户端ID的WebSocket路由

    参数:
        websocket: WebSocket连接
        client_id: 客户端ID
    """
    await websocket_endpoint(websocket, client_id)

@router.get("/api/files/{file_id}/pieces", response_model=List[Dict[str, Any]])
async def get_file_pieces(file_id: str):
    """获取文件的所有片段信息"""
    try:
        async def get_pieces(session):
            stmt = select(FilePiece).where(FilePiece.file_id == file_id)
            result = await session.execute(stmt)
            return result.scalars().all()

        pieces = await db_operation(get_pieces)
        return [piece.to_dict() for piece in pieces]
    except Exception as e:
        logging.error(f"获取文件片段失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取文件片段失败: {str(e)}")

# 启动时初始化追踪器
@router.on_event("startup")
async def startup_event():
    """应用启动时初始化追踪器"""
    tracker = get_tracker()
    manager.set_tracker(tracker)
    await tracker.start()
    logging.info("P2P追踪器已在应用启动时初始化")

# 关闭时停止追踪器
@router.on_event("shutdown")
async def shutdown_event():
    """应用关闭时停止追踪器"""
    tracker = get_tracker()
    await tracker.stop()
    logging.info("P2P追踪器已在应用关闭时停止")