"""主应用入口"""
import argparse
import asyncio
import logging
import os
import socket
import uvicorn
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse

from api_routes import router, get_tracker, manager
from certificate import get_cert_paths
from config import HOST, PORT, DEBUG
from database import init_database, get_db, AsyncSessionLocal, engine
from websocket_handler import ConnectionManager

# 配置日志
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# 创建FastAPI应用
app = FastAPI(
    title="P2P文件分发系统",
    description="用于局域网环境的P2P文件分发系统",
    version="1.0.0",
    debug=DEBUG
)

# 添加依赖覆盖
app.dependency_overrides[get_tracker] = get_tracker

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 包含API路由
app.include_router(router)

# 确保模板和静态文件目录存在
os.makedirs("templates", exist_ok=True)
os.makedirs("static", exist_ok=True)

# 设置静态文件目录和模板目录
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """首页"""
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "title": "P2P文件分发系统"}
    )

@app.get("/teacher", response_class=HTMLResponse)
async def teacher_page(request: Request):
    """教师控制台"""
    return templates.TemplateResponse(
        "teacher.html",
        {"request": request, "title": "教师控制台"}
    )

@app.get("/student", response_class=HTMLResponse)
async def student_page(request: Request):
    """学生端"""
    return templates.TemplateResponse(
        "student.html",
        {"request": request, "title": "学生端"}
    )

@app.get("/join/{distribution_id}", response_class=HTMLResponse)
async def join_distribution(request: Request, distribution_id: str):
    """加入分发任务页面"""
    return templates.TemplateResponse(
        "join.html",
        {
            "request": request,
            "title": "加入分发任务",
            "distribution_id": distribution_id
        }
    )

@app.get("/certificate-info", response_class=HTMLResponse)
async def certificate_info(request: Request):
    """证书信息页面"""
    return templates.TemplateResponse(
        "certificate_info.html",
        {"request": request, "title": "安全连接说明"}
    )

@app.get("/config", response_class=HTMLResponse)
async def config_info(request: Request):
    """配置信息"""
    # 获取本机IP地址列表
    ip_addresses = get_local_ip_addresses()

    return templates.TemplateResponse(
        "config.html",
        {
            "request": request,
            "title": "服务器配置信息",
            "host": HOST,
            "port": PORT,
            "ip_addresses": ip_addresses,
            "https_url": f"https://{ip_addresses[0]}:{PORT}" if ip_addresses else f"https://{HOST}:{PORT}"
        }
    )

def get_local_ip_addresses() -> list:
    """获取本机IP地址列表"""
    ip_list = []
    try:
        # 获取主机名
        hostname = socket.gethostname()

        # 获取主机的IP地址列表
        ips = socket.gethostbyname_ex(hostname)

        # 过滤本地回环地址
        ip_list = [ip for ip in ips[2] if not ip.startswith("127.")]

        # 如果没有找到非本地IP，添加回环地址
        if not ip_list:
            ip_list = ["127.0.0.1"]
    except Exception as e:
        logging.error(f"获取本机IP地址失败: {e}")
        ip_list = ["127.0.0.1"]

    return ip_list


# 修改启动和关闭事件处理
@app.on_event("startup")
async def startup_event():
    """应用启动时执行"""
    logging.info("正在启动P2P文件分发服务...")

    try:
        # 初始化数据库
        await init_database()

        # 获取数据库会话
        db = await get_db()

        # 初始化P2P追踪器
        tracker = get_tracker(db)

        # 设置连接管理器的追踪器
        manager.set_tracker(tracker)

        # 启动追踪器服务
        await tracker.start()

        # 输出服务信息
        ip_addresses = get_local_ip_addresses()
        ip_info = ", ".join(ip_addresses)
        logging.info(f"服务器启动于: https://{HOST}:{PORT}")
        logging.info(f"可通过以下IP访问: {ip_info}")
    except Exception as e:
        logging.error(f"启动失败: {e}", exc_info=True)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时执行"""
    logging.info("正在关闭P2P文件分发服务...")

    try:
        # 获取数据库会话
        db = await get_db()

        # 获取P2P追踪器
        tracker = get_tracker(db)

        # 停止追踪器服务
        await tracker.stop()

        # 关闭数据库连接
        await db.close()

        # 关闭数据库引擎
        await engine.dispose()
    except Exception as e:
        logging.error(f"关闭服务失败: {e}", exc_info=True)

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="P2P文件分发系统服务器")
    parser.add_argument("--host", default=HOST, help=f"监听的主机地址 (默认: {HOST})")
    parser.add_argument("--port", type=int, default=PORT, help=f"监听的端口 (默认: {PORT})")
    parser.add_argument("--debug", action="store_true", help="开启调试模式")
    args = parser.parse_args()
    
    # 生成SSL证书
    ssl_params = get_cert_paths()
    
    # 启动服务器
    uvicorn.run(
        "app:app",
        host=args.host,
        port=args.port,
        ssl_certfile=ssl_params["certfile"],
        ssl_keyfile=ssl_params["keyfile"],
        reload=args.debug
    )