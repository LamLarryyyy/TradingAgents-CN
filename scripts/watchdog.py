#!/usr/bin/env python3
"""
Backend Watchdog - 監控後端服務健康狀態，自動檢測問題並重啟

功能：
1. 定期健康檢查 (HTTP /health endpoint)
2. 檢測服務卡住 (log 文件長時間沒更新)
3. 檢測 Docker 依賴服務 (MongoDB, Redis)
4. 自動重啟後端
5. 記錄所有事件到 watchdog.log
"""

import os
import sys
import time
import signal
import subprocess
import requests
import logging
from datetime import datetime, timedelta
from pathlib import Path

# 配置
BACKEND_URL = "http://localhost:8000"
FRONTEND_URL = "http://localhost:3000"
HEALTH_ENDPOINT = f"{BACKEND_URL}/api/health"
CHECK_INTERVAL = 30  # 健康檢查間隔（秒）
HEALTH_TIMEOUT = 30  # 健康檢查超時（秒）- 後端可能在處理繁重請求
LOG_STALE_MINUTES = 10  # log 超過多少分鐘沒更新視為卡住（報告生成可能需要 20+ 分鐘）
MAX_CONSECUTIVE_FAILURES = 3  # 連續失敗多少次才重啟
RESTART_COOLDOWN = 60  # 重啟後等待多少秒再檢查

# 路徑
PROJECT_DIR = Path(__file__).parent.parent
FRONTEND_DIR = PROJECT_DIR / "frontend"
LOG_DIR = PROJECT_DIR / "logs"
BACKEND_LOG = LOG_DIR / "tradingagents.log"
WATCHDOG_LOG = LOG_DIR / "watchdog.log"
VENV_PYTHON = PROJECT_DIR / ".venv" / "bin" / "python"

# 設置 logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(WATCHDOG_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("watchdog")


class BackendWatchdog:
    def __init__(self):
        self.consecutive_failures = 0
        self.last_restart = None
        self.backend_process = None
        self.running = True
        
        # 設置信號處理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        logger.info("🛑 收到停止信號，Watchdog 關閉（後端和前端保持運行）")
        self.running = False
        # 不要停止後端和前端，讓它們繼續運行
        sys.exit(0)
    
    def check_docker_services(self) -> dict:
        """檢查 Docker 服務狀態"""
        result = {"mongodb": False, "redis": False, "docker_running": False}
        
        try:
            # 檢查 Docker daemon
            proc = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                timeout=5
            )
            result["docker_running"] = proc.returncode == 0
            
            if not result["docker_running"]:
                return result
            
            # 檢查 MongoDB（嘗試多個可能的容器名稱，用 mongo 而非 mongosh）
            mongodb_names = ["tradingagents-mongodb", "tradingagents-cn-mongodb-1", "mongodb"]
            for name in mongodb_names:
                proc = subprocess.run(
                    ["docker", "exec", name, "mongo", "--eval", "db.runCommand({ping:1})"],
                    capture_output=True,
                    timeout=10
                )
                if proc.returncode == 0:
                    result["mongodb"] = True
                    break
            
            # 檢查 Redis（嘗試多個可能的容器名稱，需要認證）
            redis_names = ["tradingagents-redis", "tradingagents-cn-redis-1", "redis"]
            redis_password = os.environ.get("REDIS_PASSWORD", "tradingagents123")
            for name in redis_names:
                proc = subprocess.run(
                    ["docker", "exec", name, "redis-cli", "-a", redis_password, "ping"],
                    capture_output=True,
                    timeout=5
                )
                if b"PONG" in proc.stdout:
                    result["redis"] = True
                    break
            
        except subprocess.TimeoutExpired:
            logger.warning("⚠️ Docker 服務檢查超時")
        except FileNotFoundError:
            logger.warning("⚠️ Docker 命令不可用")
        except Exception as e:
            logger.error(f"❌ Docker 檢查錯誤: {e}")
        
        return result
    
    def check_health_endpoint(self) -> tuple[bool, str]:
        """檢查後端健康端點"""
        try:
            response = requests.get(HEALTH_ENDPOINT, timeout=HEALTH_TIMEOUT)
            if response.status_code == 200:
                return True, "OK"
            else:
                return False, f"HTTP {response.status_code}"
        except requests.exceptions.ConnectionError:
            return False, "連接失敗 - 服務可能未運行"
        except requests.exceptions.Timeout:
            return False, "請求超時"
        except Exception as e:
            return False, f"錯誤: {str(e)}"

    def check_frontend(self) -> tuple[bool, str]:
        """檢查前端服務"""
        try:
            response = requests.get(FRONTEND_URL, timeout=HEALTH_TIMEOUT)
            if response.status_code == 200:
                return True, "OK"
            else:
                return False, f"HTTP {response.status_code}"
        except requests.exceptions.ConnectionError:
            return False, "連接失敗 - 服務可能未運行"
        except requests.exceptions.Timeout:
            return False, "請求超時"
        except Exception as e:
            return False, f"錯誤: {str(e)}"

    def find_frontend_pid(self) -> int | None:
        """找到前端進程 PID"""
        try:
            proc = subprocess.run(
                ["lsof", "-i", ":3000", "-t"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if proc.returncode == 0 and proc.stdout.strip():
                pids = proc.stdout.strip().split('\n')
                return int(pids[0])
        except Exception:
            pass
        return None

    def _stop_frontend(self):
        """停止前端進程"""
        pid = self.find_frontend_pid()
        if pid:
            logger.info(f"🛑 停止前端進程 (PID: {pid})...")
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(2)
                if self.find_frontend_pid() == pid:
                    os.kill(pid, signal.SIGKILL)
                    time.sleep(1)
            except ProcessLookupError:
                pass
            except Exception as e:
                logger.error(f"❌ 停止前端進程失敗: {e}")

    def start_frontend(self) -> bool:
        """啟動前端"""
        logger.info("🚀 啟動前端服務...")
        
        try:
            self._stop_frontend()
            time.sleep(1)
            
            env = os.environ.copy()
            subprocess.Popen(
                ["npm", "run", "dev"],
                cwd=str(FRONTEND_DIR),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )
            
            # 等待啟動
            logger.info("⏳ 等待前端啟動...")
            for i in range(30):  # 最多等 30 秒
                time.sleep(1)
                healthy, _ = self.check_frontend()
                if healthy:
                    logger.info("✅ 前端啟動成功")
                    return True
            
            logger.error("❌ 前端啟動超時")
            return False
            
        except Exception as e:
            logger.error(f"❌ 啟動前端失敗: {e}")
            return False
    
    def check_log_freshness(self) -> tuple[bool, str]:
        """檢查 log 文件是否有更新"""
        if not BACKEND_LOG.exists():
            return True, "Log 文件不存在（可能是首次啟動）"
        
        try:
            mtime = datetime.fromtimestamp(BACKEND_LOG.stat().st_mtime)
            age = datetime.now() - mtime
            
            if age > timedelta(minutes=LOG_STALE_MINUTES):
                return False, f"Log 已 {int(age.total_seconds() / 60)} 分鐘沒更新"
            return True, f"Log 最後更新: {int(age.total_seconds())} 秒前"
        except Exception as e:
            return True, f"無法檢查 log: {e}"
    
    def find_backend_pid(self) -> int | None:
        """找到後端進程 PID"""
        try:
            proc = subprocess.run(
                ["lsof", "-i", ":8000", "-t"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if proc.returncode == 0 and proc.stdout.strip():
                # 可能有多個 PID，取第一個
                pids = proc.stdout.strip().split('\n')
                return int(pids[0])
        except Exception:
            pass
        return None
    
    def _stop_backend(self):
        """停止後端進程"""
        pid = self.find_backend_pid()
        if pid:
            logger.info(f"🛑 停止後端進程 (PID: {pid})...")
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(2)
                # 如果還在運行，強制殺掉
                if self.find_backend_pid() == pid:
                    os.kill(pid, signal.SIGKILL)
                    time.sleep(1)
            except ProcessLookupError:
                pass
            except Exception as e:
                logger.error(f"❌ 停止進程失敗: {e}")
    
    def start_backend(self) -> bool:
        """啟動後端"""
        logger.info("🚀 啟動後端服務...")
        
        try:
            # 確保舊進程已停止
            self._stop_backend()
            time.sleep(1)
            
            # 啟動新進程
            env = os.environ.copy()
            self.backend_process = subprocess.Popen(
                [str(VENV_PYTHON), "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"],
                cwd=str(PROJECT_DIR),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )
            
            # 等待啟動（後端初始化需要較長時間，包括獲取股票列表）
            logger.info("⏳ 等待後端啟動（最多 180 秒）...")
            for i in range(180):  # 最多等 180 秒
                time.sleep(1)
                healthy, _ = self.check_health_endpoint()
                if healthy:
                    logger.info("✅ 後端啟動成功")
                    self.last_restart = datetime.now()
                    return True
            
            logger.error("❌ 後端啟動超時")
            return False
            
        except Exception as e:
            logger.error(f"❌ 啟動後端失敗: {e}")
            return False
    
    def diagnose_issue(self) -> str:
        """診斷問題原因"""
        issues = []
        
        # 檢查 Docker
        docker_status = self.check_docker_services()
        if not docker_status["docker_running"]:
            issues.append("Docker 未運行")
        else:
            if not docker_status["mongodb"]:
                issues.append("MongoDB 不可用")
            if not docker_status["redis"]:
                issues.append("Redis 不可用")
        
        # 檢查 log
        log_ok, log_msg = self.check_log_freshness()
        if not log_ok:
            issues.append(log_msg)
        
        # 檢查進程
        pid = self.find_backend_pid()
        if not pid:
            issues.append("後端進程不存在")
        
        # 檢查最近的錯誤 log
        try:
            error_log = LOG_DIR / "error.log"
            if error_log.exists():
                with open(error_log, 'r') as f:
                    lines = f.readlines()
                    recent_errors = [l.strip() for l in lines[-5:] if 'ERROR' in l]
                    if recent_errors:
                        issues.append(f"最近錯誤: {recent_errors[-1][:100]}")
        except Exception:
            pass
        
        return "; ".join(issues) if issues else "原因不明"
    
    def run(self):
        """主循環"""
        logger.info("=" * 60)
        logger.info("🐕 Backend Watchdog 啟動")
        logger.info(f"📍 項目目錄: {PROJECT_DIR}")
        logger.info(f"⏱️ 檢查間隔: {CHECK_INTERVAL} 秒")
        logger.info("=" * 60)
        
        # 首次檢查 Docker
        docker_status = self.check_docker_services()
        if not docker_status["docker_running"]:
            logger.error("❌ Docker 未運行！請先啟動 Docker Desktop")
            logger.info("💡 啟動後執行: docker-compose up -d mongodb redis")
            return
        
        if not docker_status["mongodb"] or not docker_status["redis"]:
            logger.warning("⚠️ MongoDB 或 Redis 未運行，嘗試啟動...")
            subprocess.run(
                ["docker-compose", "up", "-d", "mongodb", "redis"],
                cwd=str(PROJECT_DIR),
                capture_output=True
            )
            time.sleep(5)
        
        # 檢查後端是否已運行
        healthy, msg = self.check_health_endpoint()
        if not healthy:
            logger.info("📝 後端未運行，正在啟動...")
            self.start_backend()
        else:
            logger.info("✅ 後端已在運行")
        
        # 檢查前端是否已運行
        frontend_ok, frontend_msg = self.check_frontend()
        if not frontend_ok:
            logger.info("📝 前端未運行，正在啟動...")
            self.start_frontend()
        else:
            logger.info("✅ 前端已在運行")
        
        # 主監控循環
        while self.running:
            try:
                time.sleep(CHECK_INTERVAL)
                
                # 如果剛重啟，跳過檢查
                if self.last_restart and (datetime.now() - self.last_restart).total_seconds() < RESTART_COOLDOWN:
                    continue
                
                # 後端健康檢查
                healthy, msg = self.check_health_endpoint()
                log_ok, log_msg = self.check_log_freshness()
                
                # 前端健康檢查
                frontend_ok, frontend_msg = self.check_frontend()
                
                # 檢查前端，如果掛了就重啟
                if not frontend_ok:
                    logger.warning(f"⚠️ 前端異常: {frontend_msg}，嘗試重啟...")
                    self.start_frontend()
                
                # 如果 Log 還在更新，說明後端還在工作，只是忙碌
                if log_ok:
                    if not healthy:
                        # 健康檢查超時但 Log 還在更新 = 後端忙碌，不是卡住
                        logger.info(f"⏳ 後端忙碌中（健康檢查超時但 Log 還在更新）| {log_msg}")
                        self.consecutive_failures = 0
                    else:
                        self.consecutive_failures = 0
                        # 每 10 次檢查輸出一次狀態
                        if int(time.time()) % (CHECK_INTERVAL * 10) < CHECK_INTERVAL:
                            fe_status = "✅" if frontend_ok else "❌"
                            logger.info(f"✅ 後端正常 | {fe_status} 前端 | {log_msg}")
                else:
                    # Log 沒更新才算真正的問題
                    self.consecutive_failures += 1
                    logger.warning(f"⚠️ 後端檢查失敗 ({self.consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}) | 健康: {msg} | Log: {log_msg}")
                    
                    if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        # 診斷問題
                        diagnosis = self.diagnose_issue()
                        logger.error(f"❌ 連續 {MAX_CONSECUTIVE_FAILURES} 次失敗，診斷: {diagnosis}")
                        
                        # 檢查 Docker 依賴
                        docker_status = self.check_docker_services()
                        if not docker_status["docker_running"]:
                            logger.error("❌ Docker 未運行，無法重啟後端")
                            logger.info("💡 請啟動 Docker Desktop")
                            self.consecutive_failures = 0
                            continue
                        
                        if not docker_status["mongodb"] or not docker_status["redis"]:
                            logger.warning("⚠️ 重啟 Docker 服務...")
                            subprocess.run(
                                ["docker-compose", "up", "-d", "mongodb", "redis"],
                                cwd=str(PROJECT_DIR),
                                capture_output=True
                            )
                            time.sleep(5)
                        
                        # 重啟後端
                        logger.info("🔄 重啟後端...")
                        if self.start_backend():
                            self.consecutive_failures = 0
                        else:
                            logger.error("❌ 重啟失敗，等待下次檢查")
                
            except Exception as e:
                logger.error(f"❌ 監控循環錯誤: {e}")
                time.sleep(CHECK_INTERVAL)


def main():
    # 確保 logs 目錄存在
    LOG_DIR.mkdir(exist_ok=True)
    
    watchdog = BackendWatchdog()
    watchdog.run()


if __name__ == "__main__":
    main()
