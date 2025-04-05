#!/usr/bin/env python3
"""
P2P文件分发系统启动脚本
"""
import os
import sys
import argparse
import webbrowser
import platform
from time import sleep
from pathlib import Path

# 确保依赖已安装
try:
    import uvicorn
except ImportError:
    print("正在安装必要的依赖...")
    os.system(f"{sys.executable} -m pip install -r requirements.txt")
    print("依赖安装完成，正在启动...")

from config import HOST, PORT, DATA_DIR, TEMP_DIR, CERT_DIR

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="P2P文件分发系统")
    parser.add_argument("--host", default=HOST, help=f"服务器地址 (默认: {HOST})")
    parser.add_argument("--port", type=int, default=PORT, help=f"服务器端口 (默认: {PORT})")
    parser.add_argument("--no-browser", action="store_true", help="不自动打开浏览器")
    parser.add_argument("--debug", action="store_true", help="启用调试模式")
    
    args = parser.parse_args()
    
    # 确保必要的目录存在
    for directory in [DATA_DIR, TEMP_DIR, CERT_DIR, "templates", "static"]:
        os.makedirs(directory, exist_ok=True)
    
    # 启动服务器
    print(f"启动P2P文件分发系统服务器 (https://{args.host}:{args.port})...")
    print("首次启动可能需要几秒钟生成安全证书...")
    
    # 在后台自动打开浏览器
    if not args.no_browser:
        def open_browser():
            """在短暂延迟后打开浏览器"""
            sleep(2)  # 给服务器一些启动时间
            url = f"https://localhost:{args.port}"
            print(f"打开浏览器访问: {url}")
            webbrowser.open(url)
        
        # 在不同的线程中启动浏览器
        import threading
        threading.Thread(target=open_browser).start()
    
    # 启动Uvicorn服务器
    uvicorn.run(
        "app:app",
        host=args.host,
        port=args.port,
        ssl_certfile=os.path.join(CERT_DIR, "server.crt"),
        ssl_keyfile=os.path.join(CERT_DIR, "server.key"),
        reload=args.debug,
        log_level="debug" if args.debug else "info"
    )

if __name__ == "__main__":
    main()