# 多阶段构建 - 极致精简
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod main.go ./
COPY static/ ./static/

# 安装构建依赖
RUN apk add --no-cache gcc musl-dev sqlite-dev

# 下载依赖并构建
RUN go mod tidy && \
    CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o chatroom .

# 运行阶段 - 使用超轻量级镜像
FROM alpine:latest

# 只安装运行时必需的库
RUN apk add --no-cache ca-certificates sqlite-libs

WORKDIR /app

# 复制二进制文件
COPY --from=builder /app/chatroom .

# 环境变量 - 默认使用 10699 端口
ENV PORT=10699
ENV CHAT_PASSWORD=changeme
ENV GOGC=20
ENV GOMEMLIMIT=64MiB

EXPOSE 10699

# 健康检查
HEALTHCHECK --interval=60s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:10699/login || exit 1

CMD ["./chatroom"]
