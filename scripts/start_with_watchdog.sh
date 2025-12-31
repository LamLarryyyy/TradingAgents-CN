#!/bin/bash
# 使用 Watchdog 啟動後端服務
# 會自動監控健康狀態並在卡住時重啟

cd "$(dirname "$0")/.."
PROJECT_DIR=$(pwd)

echo "🐕 啟動 Backend Watchdog..."
echo "📍 項目目錄: $PROJECT_DIR"
echo ""

# 檢查 Docker
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker 未運行！請先啟動 Docker Desktop"
    exit 1
fi

# 啟動 Docker 服務
echo "🐳 啟動 Docker 服務 (MongoDB, Redis)..."
docker-compose up -d mongodb redis
sleep 3

# 啟動 Watchdog
echo "🚀 啟動 Watchdog..."
source .venv/bin/activate
python scripts/watchdog.py
