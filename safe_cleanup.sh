#!/bin/bash

echo "🔧 釋放 Docker build cache..."
docker builder prune -a -f

echo "🧹 清理 Docker 未使用資源..."
docker system prune -a --volumes -f

echo "🧹 清理 apt 快取與暫存檔..."
sudo apt-get clean
sudo rm -rf /var/lib/apt/lists/*
sudo rm -rf /tmp/*

echo "🧹 嘗試清除 Airflow logs（若存在）..."
if [ -d "./logs" ]; then
    rm -rf ./logs/*
    echo "✅ ./logs 已清除"
fi

echo "🧠 檢查佔用最多空間的檔案..."
sudo du -ah / | sort -rh | head -n 20

echo "✅ 清理完成！目前磁碟狀況："
df -h /

#每日清理