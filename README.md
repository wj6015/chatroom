# 极简实时聊天室 (单端口 10699 版本)

为 128MB 内存 Alpine Linux NAT VPS 极致优化的 WebSocket 聊天室。

## 特性

- **单端口运行**: 只使用 10699 TCP 端口
- **极致轻量**: 运行内存 < 64MB，支持 50+ 并发连接
- **无需客户端**: 纯 Web 访问，支持手机/PC
- **密码保护**: 入口页面密码认证
- **匿名/昵称**: 可匿名聊天，也可设置昵称
- **私信功能**: 点击在线用户列表即可私信
- **历史记录**: 保留最近 30 条消息
- **零配置**: 单文件 SQLite 数据库

## 系统要求

- Alpine Linux (或任何 Linux)
- 128MB+ 内存
- 512MB+ 存储
- **1个可用 TCP 端口 (默认 10699)**
- Go 1.21+ (仅编译时需要)

## 部署步骤

### 1. 上传文件到 VPS

将以下文件上传到 VPS 的 `/root` 目录：
- `main.go`
- `go.mod`
- `static/index.html`
- `deploy.sh`

```bash
# 在本地执行
scp main.go go.mod deploy.sh root@your-vps-ip:/root/
scp static/index.html root@your-vps-ip:/root/static/
```

### 2. 执行部署脚本

```bash
ssh root@your-vps-ip
cd /root
chmod +x deploy.sh
./deploy.sh
```

### 3. 设置密码并启动

```bash
# 设置访问密码（重要！）
export CHAT_PASSWORD=your_secure_password

# 启动服务
rc-service chatroom start

# 设置开机自启
rc-update add chatroom default
```

### 4. 访问聊天室

浏览器打开：
```
http://your-nat-domain-or-ip:10699
```

输入密码后即可进入聊天室。

## 端口说明

本系统只使用 **10699** 一个 TCP 端口：
- `/` - 聊天室主页面 (HTTP)
- `/login` - 登录页面 (HTTP)
- `/ws` - WebSocket 实时通信

所有功能都通过这一个端口提供，无需其他端口。

## 内存优化技术

针对 128MB 小鸡的优化：

1. **使用 coder/websocket**: 比 gorilla/websocket 更轻量
2. **禁用 WebSocket 压缩**: 节省内存
3. **积极 GC**: GOGC=20 更频繁回收内存
4. **硬内存限制**: GOMEMLIMIT=64MiB
5. **小缓冲设计**: 消息通道缓冲仅 16
6. **SQLite 优化**: WAL 模式，限制连接池

## 性能指标

- **空闲内存**: ~20MB
- **每连接占用**: ~2KB
- **并发支持**: 50-100 人
- **编译后大小**: ~15MB
- **只使用 1 个 TCP 端口**

## 自定义配置

编辑 `/opt/chatroom/start.sh` 修改环境变量：

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `PORT` | 监听端口 | **10699** |
| `CHAT_PASSWORD` | 访问密码 | changeme |
| `GOGC` | GC 频率 | 20 |
| `GOMEMLIMIT` | 内存限制 | 64MiB |

**注意**: 如果你的 NAT 提供商分配的端口不是 10699，修改 `PORT` 变量即可。

## Docker 部署（可选）

如果你的 VPS 有 Docker：

```bash
export CHAT_PASSWORD=your_secure_password
docker-compose up -d
```

## 故障排查

1. **无法访问**: 检查 NAT 提供商是否正确转发 10699 端口到 VPS
2. **端口冲突**: 确保 10699 端口未被其他程序占用
3. **内存不足**: 增加 SWAP 分区
4. **连接断开**: 检查防火墙是否放行 10699 端口

## 使用说明

1. **设置昵称**: 进入后在顶部输入框设置昵称
2. **公开聊天**: 默认在公共频道
3. **私信**: 点击左侧在线用户列表中的名字
4. **切换回公共**: 点击输入框上方的 `[切换]` 链接
