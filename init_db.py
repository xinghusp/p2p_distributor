"""数据库初始化脚本"""
import asyncio
import logging
import os
import sys
from sqlalchemy import text

# 设置日志级别
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# 导入必要的模块
from config import DATABASE_URL
from database import Base, engine


async def init_db():
    """初始化数据库"""
    try:
        logging.info(f"使用数据库URL: {DATABASE_URL}")

        # 确保数据库目录存在
        if not DATABASE_URL.startswith('sqlite+aiosqlite:///:memory:'):
            db_path = DATABASE_URL.replace('sqlite+aiosqlite:///', '')
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
                logging.info(f"创建数据库目录: {db_dir}")

        logging.info("开始创建数据库表...")

        # 创建数据库表
        async with engine.begin() as conn:
            # 删除所有现有表以确保干净的状态
            await conn.run_sync(Base.metadata.drop_all)
            logging.info("已删除所有现有表")

            # 创建所有表
            await conn.run_sync(Base.metadata.create_all)
            logging.info("已创建所有表")

            # 配置SQLite优化
            await conn.execute(text("PRAGMA journal_mode=WAL;"))
            await conn.execute(text("PRAGMA temp_store=MEMORY;"))
            await conn.execute(text("PRAGMA read_uncommitted=1;"))
            await conn.execute(text("PRAGMA synchronous=NORMAL;"))
            await conn.execute(text("PRAGMA cache_size=10000;"))
            logging.info("已应用SQLite优化配置")

        # 列出所有创建的表
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
            tables = [row[0] for row in result]
            logging.info(f"成功创建的表: {', '.join(tables)}")

        logging.info("数据库初始化完成!")
        return True
    except Exception as e:
        logging.critical(f"数据库初始化失败: {e}", exc_info=True)
        return False
    finally:
        await engine.dispose()


if __name__ == "__main__":
    success = asyncio.run(init_db())
    sys.exit(0 if success else 1)