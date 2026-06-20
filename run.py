import sys
import os
import asyncio
import logging
import subprocess
import signal
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

logger = logging.getLogger(__name__)


def kill_port(port=8000):
    """启动前清理占用端口的旧进程"""
    try:
        if sys.platform == 'win32':
            result = subprocess.run(
                ['netstat', '-ano'], capture_output=True, text=True
            )
            for line in result.stdout.splitlines():
                if f':{port}' in line and 'LISTENING' in line:
                    pid = line.strip().split()[-1]
                    subprocess.run(['taskkill', '/F', '/PID', pid],
                                   capture_output=True, timeout=5)
                    logger.info(f'已终止占用端口 {port} 的进程 PID={pid}')
        else:
            result = subprocess.run(
                ['fuser', f'{port}/tcp'], capture_output=True, text=True
            )
            pids = result.stdout.strip().split()
            for pid in pids:
                pid = pid.strip()
                if pid.isdigit():
                    os.kill(int(pid), signal.SIGTERM)
                    logger.info(f'已终止占用端口 {port} 的进程 PID={pid}')
    except Exception as e:
        logger.warning(f'端口清理失败: {e}（可忽略）')


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger.info('=' * 50)
    logger.info('  基于HDFS+Hive+Spark的全国蔬菜产业智能分析平台')
    logger.info('=' * 50)

    from src.data_processing.processor import DataProcessor
    DataProcessor.get_instance()

    # 启动前清理端口冲突
    kill_port(8000)

    import uvicorn
    config = uvicorn.Config('src.api.app:app', host='0.0.0.0', port=8000, reload=False)
    server = uvicorn.Server(config)

    async def run_all():
        await server.serve()

    logger.info('启动 Web 服务: http://localhost:8000')
    logger.info('按 Ctrl+C 停止')
    asyncio.run(run_all())


if __name__ == '__main__':
    main()
