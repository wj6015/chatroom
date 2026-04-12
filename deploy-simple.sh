#!/bin/sh
# Alpine Linux 128MB VPS 简化部署脚本（使用预编译二进制）

set -e

echo "=== 聊天室简化部署脚本 ==="
echo "适用于 128MB 内存 + 512MB 存储的 NAT VPS"
echo "端口: 10699"

# 检查root权限
if [ "$(id -u)" != "0" ]; then
   echo "请使用 root 权限运行: sudo sh deploy.sh"
   exit 1
fi

# 创建应用目录
echo "[1/3] 创建目录..."
mkdir -p /opt/chatroom
cd /opt/chatroom

# 检查二进制文件是否存在
if [ ! -f "/root/chatroom" ]; then
    echo "错误: 未找到 chatroom 二进制文件"
    echo "请确保已将编译好的 chatroom 文件上传到 /root 目录"
    exit 1
fi

# 复制二进制文件
echo "[2/3] 安装程序..."
cp /root/chatroom /opt/chatroom/
chmod +x /opt/chatroom/chatroom

# 创建启动脚本
echo "[3/3] 配置启动脚本..."
cat > /opt/chatroom/start.sh << 'EOF'
#!/bin/sh
# 聊天室启动脚本 - 端口 10699

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

echo ""
echo "=== 部署完成 ==="
echo ""
echo "使用方法:"
echo "  1. 设置密码: export CHAT_PASSWORD=your_password"
echo "  2. 启动服务: rc-service chatroom start"
echo "  3. 开机自启: rc-update add chatroom default"
echo "  4. 访问: http://your-nat-domain:10699"
echo ""
echo "或者直接运行:"
echo "  cd /opt/chatroom && ./start.sh"
echo ""
