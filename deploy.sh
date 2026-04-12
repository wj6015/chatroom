#!/bin/sh
# Alpine Linux 128MB VPS 部署脚本 (单端口 10699 版本)

set -e

echo "=== 聊天室部署脚本 (Alpine Linux) ==="
echo "适用于 128MB 内存的 NAT VPS (端口: 10699)"

# 检查root权限
if [ "$(id -u)" != "0" ]; then
   echo "请使用 root 权限运行: sudo sh deploy.sh"
   exit 1
fi

cd /root

# 1. 安装必要包（最小化安装）
echo "[1/5] 安装依赖..."
apk update
apk add --no-cache go gcc musl-dev sqlite-dev git

# 2. 创建应用目录
echo "[2/5] 创建目录..."
mkdir -p /opt/chatroom

# 3. 复制源码
echo "[3/5] 准备源码..."
cp /root/main.go /opt/chatroom/
cp /root/go.mod /opt/chatroom/
mkdir -p /opt/chatroom/static
cp /root/static/index.html /opt/chatroom/static/

# 4. 编译（极致优化）
echo "[4/5] 编译程序..."
cd /opt/chatroom

# 设置编译参数优化内存
export CGO_ENABLED=1
export GOOS=linux
export GOARCH=amd64

# 下载依赖
go mod tidy

# 编译：剥离符号表，减小体积，优化内存
go build -ldflags="-s -w -extldflags '-static'" -o chatroom .

# 清理不需要的文件
rm -f go.mod go.sum main.go
rm -rf static

# 5. 创建启动脚本
echo "[5/5] 配置启动..."
cat > /opt/chatroom/start.sh << 'EOF'
#!/bin/sh
# 聊天室启动脚本 - 使用端口 10699

# 设置环境变量
export PORT=10699
export CHAT_PASSWORD=${CHAT_PASSWORD:-"changeme"}

# 内存优化设置
export GOGC=20                 # 更积极的GC
export GOMEMLIMIT=64MiB        # 限制Go内存使用

cd /opt/chatroom
exec ./chatroom
EOF

chmod +x /opt/chatroom/start.sh
chmod +x /opt/chatroom/chatroom

# 创建 OpenRC 服务
cat > /etc/init.d/chatroom << 'EOF'
#!/sbin/openrc-run

description="Lightweight Chatroom on port 10699"

command="/opt/chatroom/start.sh"
command_background=true
pidfile="/run/chatroom.pid"
directory="/opt/chatroom"

export PORT="10699"
export CHAT_PASSWORD="${CHAT_PASSWORD:-changeme}"
export GOGC="20"
export GOMEMLIMIT="64MiB"

depend() {
    need net
    after firewall
}
EOF

chmod +x /etc/init.d/chatroom

# 清理构建依赖（可选，节省空间）
echo ""
echo "是否删除构建依赖以节省空间？(y/n)"
read -r response
if [ "$response" = "y" ]; then
    apk del go gcc musl-dev git
    echo "已删除构建依赖"
fi

echo ""
echo "=== 部署完成 ==="
echo ""
echo "使用方法:"
echo "  1. 设置密码: export CHAT_PASSWORD=your_password"
echo "  2. 启动服务: rc-service chatroom start"
echo "  3. 开机自启: rc-update add chatroom default"
echo "  4. 访问: http://your-vps-ip:10699"
echo ""
echo "或者直接运行:"
echo "  cd /opt/chatroom && ./start.sh"
echo ""
echo "注意: 确保你的 NAT 提供商已将 10699 端口转发到这台 VPS"
echo ""
