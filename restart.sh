#!/bin/sh
# ============================================================
# Silievo 网关 - 重启脚本（停旧 + 启动 + 健康检查）
#
# 用法：
#   sh restart.sh            # 生产环境（prod）重启，等待 /health 通过
#   sh restart.sh dev        # 本地开发环境（dev）调试
#
# 做三件事：
#   1. 停止旧进程（按 pid 文件 + 按端口 3003 兜底，双保险）
#   2. nohup java -jar 后台启动，pid 写入 logs/gateway.pid
#   3. 轮询 http://127.0.0.1:3003/health 直到 200，最多 90 秒
#
# 前置：JDK 21；jar 在本目录或 target/ 子目录。
# 兼容性：POSIX sh（dash/bash/sh 均可执行）。
# ============================================================
set -eu
cd "$(dirname "$0")"

JAR_NAME="pinche-gateway-1.0-SNAPSHOT.jar"
# 优先 target/ 子目录，否则取脚本同目录
if [ -f "target/$JAR_NAME" ]; then
  JAR="target/$JAR_NAME"
else
  JAR="$JAR_NAME"
fi
PORT="3003"
PROFILE="prod"
HEALTH_URL="http://127.0.0.1:${PORT}/health"
TIMEOUT_SEC="90"
POLL_INTERVAL="3"
PID_FILE="logs/gateway.pid"

if [ "${1:-}" = "dev" ]; then
  PROFILE="dev"
fi

# ---- 1. 停止旧进程 ----
echo "==> 停止旧进程..."

# 1a. 按 pid 文件停止（上次正常启动记录的场景）
if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "   按 pid 文件停止 (pid $OLD_PID)..."
    kill "$OLD_PID"
    sleep 2
    if kill -0 "$OLD_PID" 2>/dev/null; then
      kill -9 "$OLD_PID" 2>/dev/null || true
    fi
    echo "   ✓ 已停止 (pid $OLD_PID)"
  else
    echo "   pid 文件中的进程 ($OLD_PID) 已不存在。"
  fi
  rm -f "$PID_FILE"
fi

# 1b. 按端口兜底：杀掉仍占用 3003 的进程
#     （手动启动 / 上次异常退出遗留的无 pid 文件进程，都能被清掉）
if command -v fuser >/dev/null 2>&1; then
  if fuser -k -n tcp "$PORT" >/dev/null 2>&1; then
    echo "   ✓ 已释放被旧进程占用的端口 $PORT"
  else
    echo "   端口 $PORT 空闲。"
  fi
  sleep 2
elif command -v lsof >/dev/null 2>&1; then
  PIDS=$(lsof -ti tcp:"$PORT" 2>/dev/null || true)
  if [ -n "$PIDS" ]; then
    echo "   释放被旧进程占用的端口 $PORT..."
    kill $PIDS 2>/dev/null || true
    sleep 2
  else
    echo "   端口 $PORT 空闲。"
  fi
fi

# ---- 2. 后台启动 ----
echo "==> 启动网关（profile=$PROFILE, port=$PORT）..."
mkdir -p logs
nohup java -jar "$JAR" --spring.profiles.active="$PROFILE" > logs/gateway.log 2>&1 &
echo $! > "$PID_FILE"
NEW_PID=$(cat "$PID_FILE")
echo "   pid: $NEW_PID，日志：logs/gateway.log"

# ---- 3. 等待健康检查通过 ----
echo "==> 等待健康检查（最多 ${TIMEOUT_SEC}s）..."
elapsed=0
while [ "$elapsed" -lt "$TIMEOUT_SEC" ]; do
  # 新进程已退出（启动失败，如端口冲突）→ 直接报错，不再等健康检查
  if ! kill -0 "$NEW_PID" 2>/dev/null; then
    echo "❌ 新进程 (pid $NEW_PID) 已退出，启动失败。最近日志："
    tail -30 logs/gateway.log 2>/dev/null || true
    exit 1
  fi
  if curl -sf "$HEALTH_URL" >/dev/null 2>&1; then
    echo "   ✓ 网关已就绪（耗时 ${elapsed}s）：$HEALTH_URL"
    echo ""
    echo "✅ 重启完成。"
    echo "   实时日志：tail -f logs/gateway.log"
    exit 0
  fi
  sleep "$POLL_INTERVAL"
  elapsed=$((elapsed + POLL_INTERVAL))
done

echo "❌ 等待 ${TIMEOUT_SEC}s 后 /health 仍未通过。最近日志："
tail -30 logs/gateway.log 2>/dev/null || true
exit 1
