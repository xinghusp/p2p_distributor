"""配置文件"""
import os
from pathlib import Path

# 基础目录配置
BASE_DIR = Path(__file__).parent.absolute()
DATA_DIR = BASE_DIR / "data"
TEMP_DIR = BASE_DIR / "temp"
CERT_DIR = BASE_DIR / "certificates"

# 确保必要目录存在
for directory in [DATA_DIR, TEMP_DIR, CERT_DIR]:
    os.makedirs(directory, exist_ok=True)

# 数据库配置
DATABASE_URL = f"sqlite+aiosqlite:///{DATA_DIR}/p2p_distributor.db"

# 服务器配置
HOST = "0.0.0.0"  # 监听所有网络接口
PORT = 8443
DEBUG = os.getenv("DEBUG", "False").lower() == "true"

# 文件分发配置
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1MB 分片大小
MAX_PEERS_PER_CLIENT = 10  # 每个客户端最大连接的peer数
TRACKER_UPDATE_INTERVAL = 5  # tracker状态更新间隔(秒)

# 证书配置
CERT_FILE = CERT_DIR / "server.crt"
KEY_FILE = CERT_DIR / "server.key"