# 本地编译说明

由于你的 VPS 只有 512MB 存储，无法安装 Go 编译器（需要 200MB+ 空间），
你需要在本地（电脑或其他 Linux 机器）编译好二进制文件，然后上传到 VPS。

## 方法一：使用 Docker 编译（推荐，最简单）

在你的电脑（Windows/Mac/Linux 都可以）上安装 Docker，然后执行：

```bash
# 1. 进入项目目录
cd chatroom

# 2. 使用 Docker 编译 Alpine 可用的静态二进制
docker run --rm -v "$PWD":/app -w /app golang:1.21-alpine sh -c "apk add --no-cache gcc musl-dev sqlite-dev && go mod tidy && CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -ldflags='-s -w -extldflags -static' -o chatroom ."

# 3. 编译完成后，chatroom 二进制文件就在当前目录
ls -lh chatroom
```

## 方法二：在另一台 Linux 服务器上编译

如果你有其他 Linux 服务器（如 Ubuntu/Debian）：

```bash
# 1. 安装 musl 工具链
sudo apt-get install musl-tools

# 2. 进入项目目录，编译
export CC=musl-gcc
go mod tidy
CGO_ENABLED=1 CC=musl-gcc GOOS=linux GOARCH=amd64 go build -ldflags='-s -w -linkmode external -extldflags -static' -o chatroom .
```

## 上传到 VPS

编译完成后，将二进制文件上传到 VPS：

```bash
# 本地执行
scp chatroom root@your-vps-ip:/root/
scp deploy-simple.sh root@your-vps-ip:/root/

# SSH 到 VPS 执行部署
ssh root@your-vps-ip
cd /root
chmod +x deploy-simple.sh
./deploy-simple.sh
```

## 启动服务

```bash
export CHAT_PASSWORD=your_secure_password
rc-service chatroom start
rc-update add chatroom default
```
