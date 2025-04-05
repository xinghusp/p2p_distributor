"""文件处理模块"""
import asyncio
import hashlib
import logging
import os
import uuid
from pathlib import Path
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from database import FileInfo, FilePiece, db_operation

class FileProcessor:
    """文件处理器"""

    def __init__(self):
        """初始化文件处理器 - 不再存储数据库会话"""
        self.chunk_size = 1024 * 1024  # 默认分片大小：1MB

    async def _calculate_file_hash(self, file_path: Path) -> str:
        """
        计算文件SHA256散列值

        参数:
            file_path: 文件路径

        返回:
            str: SHA256散列值（十六进制字符串）
        """
        sha256_hash = hashlib.sha256()

        with open(file_path, "rb") as f:
            # 分块读取文件以避免内存问题
            for chunk in iter(lambda: f.read(8192), b""):
                sha256_hash.update(chunk)

        return sha256_hash.hexdigest()

    async def _calculate_chunk_hash(self, file_path: Path, offset: int, size: int) -> str:
        """
        计算文件分片的SHA256散列值

        参数:
            file_path: 文件路径
            offset: 分片在文件中的偏移量
            size: 分片大小

        返回:
            str: SHA256散列值（十六进制字符串）
        """
        sha256_hash = hashlib.sha256()

        with open(file_path, "rb") as f:
            f.seek(offset)
            data = f.read(size)
            sha256_hash.update(data)

        return sha256_hash.hexdigest()

    async def _create_file_pieces(self, file_path: Path, file_id: str) -> List[FilePiece]:
        """
        创建文件分片

        参数:
            file_path: 文件路径
            file_id: 文件ID

        返回:
            List[FilePiece]: 分片列表
        """
        file_size = file_path.stat().st_size
        pieces = []

        # 如果文件小于分片大小，则只创建一个分片
        if file_size <= self.chunk_size:
            piece_hash = await self._calculate_chunk_hash(file_path, 0, file_size)
            pieces.append(FilePiece(
                id=str(uuid.uuid4()),
                file_id=file_id,
                index=0,
                offset=0,
                size=file_size,
                sha256=piece_hash
            ))
        else:
            # 创建多个分片
            offset = 0
            index = 0

            while offset < file_size:
                piece_size = min(self.chunk_size, file_size - offset)
                piece_hash = await self._calculate_chunk_hash(file_path, offset, piece_size)

                pieces.append(FilePiece(
                    id=str(uuid.uuid4()),
                    file_id=file_id,
                    index=index,
                    offset=offset,
                    size=piece_size,
                    sha256=piece_hash
                ))

                offset += piece_size
                index += 1

        return pieces

    async def process_file(self, file_path: str, file_name: str, distribution_id: str,
                           priority: int = 0) -> FileInfo:
        """
        处理文件，生成分片和元数据并保存文件

        参数:
            file_path: 临时文件路径
            file_name: 文件名
            distribution_id: 分发任务ID
            priority: 优先级

        返回:
            FileInfo: 文件信息对象
        """
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        # 计算文件SHA256散列值
        file_hash = await self._calculate_file_hash(file_path_obj)
        file_size = file_path_obj.stat().st_size

        # 确保目标目录存在
        target_dir = os.path.join("uploads", distribution_id)
        os.makedirs(target_dir, exist_ok=True)

        # 构建目标文件路径
        target_file_path = os.path.join(target_dir, file_name)

        # 确保目标文件的父目录存在 (如果file_name包含路径)
        os.makedirs(os.path.dirname(os.path.join(target_dir, file_name)), exist_ok=True)

        # 复制文件到永久存储位置
        logging.info(f"复制文件从 {file_path} 到 {target_file_path}")
        import shutil
        shutil.copy2(file_path, target_file_path)
        logging.info(f"文件复制成功，大小: {file_size} 字节")

        # 创建文件信息和分片记录 - 使用数据库操作包装器
        async def _create_file_info(session):
            # 创建文件信息记录
            file_info = FileInfo(
                id=str(uuid.uuid4()),
                distribution_id=distribution_id,
                path=file_name,
                size=file_size,
                priority=priority,
                sha256=file_hash
            )

            session.add(file_info)
            await session.flush()

            # 处理文件分片
            pieces = await self._create_file_pieces(file_path_obj, file_info.id)
            session.add_all(pieces)

            return file_info

        # 使用数据库操作包装器执行
        try:
            return await db_operation(_create_file_info)
        except Exception as e:
            # 如果数据库操作失败，删除已复制的文件
            if os.path.exists(target_file_path):
                try:
                    os.unlink(target_file_path)
                    logging.warning(f"由于数据库错误，已删除文件: {target_file_path}")
                except Exception as del_err:
                    logging.warning(f"删除文件失败: {del_err}")
            logging.error(f"处理文件出错: {e}", exc_info=True)
            raise