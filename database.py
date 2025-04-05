"""数据库模块"""
import json
import logging
import os
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional, AsyncGenerator, Awaitable, Callable, TypeVar

from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, func, text, Text, Boolean, Float
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.pool import NullPool
from datetime import datetime
import uuid

import config

#from config import DATABASE_DIR, DATABASE_NAME

DATABASE_URL = config.DATABASE_URL
# 创建模型基类
Base = declarative_base()

# 创建引擎
engine = create_async_engine(
    DATABASE_URL,
    connect_args={
        "timeout": 60,
        "check_same_thread": False,
        "isolation_level": "IMMEDIATE"  # 更严格的隔离级别
    },
    poolclass=NullPool,  # 禁用连接池
    echo=config.DEBUG
)

# 创建会话工厂
AsyncSessionMaker = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False  # 避免自动刷新导致的意外查询
)

# 定义泛型返回类型
T = TypeVar('T')

# 数据库操作信号量 - 限制到一个并发操作以避免锁竞争
_db_semaphore = asyncio.Semaphore(1)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """获取一个新的数据库会话"""
    session = AsyncSessionMaker()
    try:
        yield session
    finally:
        await session.close()


async def db_operation(func: Callable[[AsyncSession], Awaitable[T]]) -> T:
    """
    执行数据库操作的装饰器

    参数:
        func: 接受数据库会话的异步函数

    返回:
        func的返回值
    """
    # 使用信号量限制并发
    async with _db_semaphore:
        async with get_session() as session:
            try:
                # 创建事务
                async with session.begin():
                    # 执行操作
                    return await func(session)
            except Exception as e:
                logging.error(f"数据库操作失败: {e}", exc_info=True)
                raise


async def init_database():
    """初始化数据库"""
    try:
        logging.info("开始初始化数据库...")

        # 创建所有表
        async with engine.begin() as conn:
            logging.info("创建数据库表...")
            await conn.run_sync(Base.metadata.create_all)
            logging.info("数据库表创建完成")

            # 配置SQLite优化
            logging.info("应用SQLite优化配置...")
            await conn.execute(text("PRAGMA journal_mode=WAL;"))
            await conn.execute(text("PRAGMA temp_store=MEMORY;"))
            await conn.execute(text("PRAGMA read_uncommitted=1;"))
            await conn.execute(text("PRAGMA synchronous=NORMAL;"))
            await conn.execute(text("PRAGMA cache_size=10000;"))
            logging.info("SQLite优化配置已应用")

        logging.info("数据库初始化完成")
        return True
    except Exception as e:
        logging.error(f"数据库初始化失败: {e}", exc_info=True)
        # 抛出异常以确保应用不会在数据库初始化失败的情况下继续运行
        raise


# ====== 数据库模型定义 ======

class Distribution(Base):
    """文件分发任务表"""
    __tablename__ = "distributions"

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=func.now())
    status = Column(String(50), default="active")  # active, completed, paused, cancelled
    total_size = Column(Integer, default=0)  # 总字节数

    # 关系
    files = relationship("FileInfo", back_populates="distribution", cascade="all, delete-orphan")
    peers = relationship("Peer", back_populates="distribution", cascade="all, delete-orphan")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "name": self.name,
            "created_at": self.created_at.isoformat(),
            "status": self.status,
            "total_size": self.total_size,
            "files": [file.to_dict() for file in self.files],
            "peer_count": len(self.peers)
        }


class FileInfo(Base):
    """文件信息表"""
    __tablename__ = "files"

    id = Column(String(36), primary_key=True)
    distribution_id = Column(String(36), ForeignKey("distributions.id"), nullable=False)
    path = Column(String(1024), nullable=False)
    size = Column(Integer, nullable=False)
    priority = Column(Integer, default=0)
    sha256 = Column(String(64), nullable=True)

    # 关系
    distribution = relationship("Distribution", back_populates="files")
    pieces = relationship("FilePiece", back_populates="file", cascade="all, delete-orphan")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "path": self.path,
            "size": self.size,
            "priority": self.priority,
            "sha256": self.sha256,
            "piece_count": len(self.pieces)
        }


class FilePiece(Base):
    """文件分片表"""
    __tablename__ = "file_pieces"

    id = Column(String(36), primary_key=True)
    file_id = Column(String(36), ForeignKey("files.id"), nullable=False)
    index = Column(Integer, nullable=False)
    offset = Column(Integer, nullable=False)
    size = Column(Integer, nullable=False)
    sha256 = Column(String(64), nullable=True)

    # 关系
    file = relationship("FileInfo", back_populates="pieces")
    peer_pieces = relationship("PeerPiece", back_populates="piece")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "index": self.index,
            "offset": self.offset,
            "size": self.size,
            "sha256": self.sha256
        }


class Peer(Base):
    """客户端节点表"""
    __tablename__ = "peers"

    id = Column(String(36), primary_key=True)
    distribution_id = Column(String(36), ForeignKey("distributions.id"), nullable=False)
    client_id = Column(String(36), nullable=False)
    ip_address = Column(String(45), nullable=False)
    port = Column(Integer, nullable=False)
    user_agent = Column(String(255), nullable=True)
    joined_at = Column(DateTime, default=func.now())
    last_seen = Column(DateTime, default=func.now())
    is_seed = Column(Boolean, default=False)
    upload_speed = Column(Float, default=0.0)  # 字节/秒
    download_speed = Column(Float, default=0.0)  # 字节/秒

    # 关系
    distribution = relationship("Distribution", back_populates="peers")
    peer_pieces = relationship("PeerPiece", back_populates="peer", cascade="all, delete-orphan")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "client_id": self.client_id,
            "ip_address": self.ip_address,
            "port": self.port,
            "joined_at": self.joined_at.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "is_seed": self.is_seed,
            "upload_speed": self.upload_speed,
            "download_speed": self.download_speed,
            "progress": self.get_progress()
        }

    def get_progress(self) -> float:
        """计算下载进度"""
        if not self.peer_pieces:
            return 0.0

        total_files_size = sum(file.size for file in self.distribution.files)
        if total_files_size == 0:
            return 0.0

        downloaded_size = sum(piece.piece.size for piece in self.peer_pieces if piece.has_piece)
        return min(1.0, downloaded_size / total_files_size)


class PeerPiece(Base):
    """节点持有的分片关系表"""
    __tablename__ = "peer_pieces"

    id = Column(String(36), primary_key=True)
    peer_id = Column(String(36), ForeignKey("peers.id"), nullable=False)
    piece_id = Column(String(36), ForeignKey("file_pieces.id"), nullable=False)
    has_piece = Column(Boolean, default=False)
    last_updated = Column(DateTime, default=func.now())

    # 关系
    peer = relationship("Peer", back_populates="peer_pieces")
    piece = relationship("FilePiece", back_populates="peer_pieces")


class TrackerEvent(Base):
    """Tracker事件日志表"""
    __tablename__ = "tracker_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    distribution_id = Column(String(36), ForeignKey("distributions.id"), nullable=True)
    peer_id = Column(String(36), ForeignKey("peers.id"), nullable=True)
    event_type = Column(String(50), nullable=False)  # announce, scrape, error, etc.
    timestamp = Column(DateTime, default=func.now())
    details = Column(Text, nullable=True)  # JSON格式的详细信息

    def set_details(self, details_dict: Dict[str, Any]) -> None:
        """设置JSON格式的详细信息"""
        self.details = json.dumps(details_dict)

    def get_details(self) -> Dict[str, Any]:
        """获取详细信息字典"""
        if not self.details:
            return {}
        return json.loads(self.details)
