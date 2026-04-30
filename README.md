这是一个专为 **低配 VPS**（如 64M/128M 内存的 NAT VPS）设计的轻量级即时通讯系统。采用 Go 语言开发，具备高性能、低延迟和强隐私保护的特点。

## ✨ 核心特性

* **极低资源占用**：在 Alpine Linux 环境下运行仅需约 10MB 内存，非常适合 64M 内存的极小机型。
                 可以通过CF 的“源站规则”映射端口（零消耗，最推荐）签发ssl证书实现https加密数据通讯
* **双重身份认证**：
    * **第一层 (门禁)**：全局访问密码，过滤非授权用户。
    * **第二层 (身份)**：个人昵称 + 密码。新昵称自动注册，老昵称验密登录。
* **隐私隔离**：私聊记录基于“昵称+密码”双重绑定，即便退出后他人冒用相同昵称，只要密码不对也无法查看历史记录。
* **三段式 Web UI**：
    * 固定顶栏（状态显示）。
    * 自适应消息区（流畅滚动，不遮挡）。
    * 固定底部输入框（针对移动端软键盘进行了优化）。
* **全静态链接编译**：针对 Alpine Linux 的 `musl libc` 环境进行了特殊处理，解决 `fcntl64` 等兼容性报错。
* **新增@消息提供系统**：针对公共频道@用户信息提供功能，可以单选用户/多选/全选用户提醒
---

## 🛠 部署指南

### 1. 编译构建
本项目推荐使用 GitHub Actions 进行自动化编译，以获得最佳的 Alpine 兼容性：
1. 将代码推送至 GitHub 仓库。
2. 在项目页面的 **Actions** 标签中找到最新的构建记录。
3. 下载名为 `chatroom-binary` 的 Artifact。
4. 解压获得 `chatroom` 二进制文件。

### 2. 上传文件/部署/结束进程/维护
在你的本地终端（macOS/Linux）执行：
```bash
scp ./chatroom -P 22 root@你的VPS_IP:/root/  #上传文件
apk add --no-cache musl libc6-compat #配置环境
chmod +x /root/chatroom #赋权
export PORT=你的端口号
./chatroom #测试
nohup /root/chatroom > /root/chat.log 2>&1 & #后台运行
pkill chatroom #结束进程
pkill -9 chatroom #强力结束进程
ps aux | grep chatroom #检查是否结束进程。如果只看到 grep 这一行，说明 chatroom 已经消失了
rm chat.db #清空历史/重置密码
tail -f chat.log #查看运行日志

邀请码系统维护：保证invite_codes.txt与chatroom在同一目录下
（1）新增邀请码
pkill chatroom #结束进程
vi invite_codes.txt #打开邀请码文件
例：NEW-USER-001 #新增一行
 ./chatroom #重启

（2）禁用邀请码
sqlite3 chat.db "
UPDATE invite_codes
SET disabled = 1
WHERE code = 'NEW-USER-001';
"

（3）删除邀请码
sqlite3 chat.db "
DELETE FROM invite_codes
WHERE code = 'NEW-USER-001';
"

（4）查看邀请码状态
sqlite3 chat.db "
SELECT
  code,
  used_by,
  datetime(used_at, 'unixepoch', 'localtime') AS used_time,
  disabled
FROM invite_codes;
"

（5）查看所有表
sqlite3 chat.db ".tables"

（6）查看 invite_codes 表结构
sqlite3 chat.db ".schema invite_codes"

（7）查看 users
sqlite3 chat.db "
SELECT username FROM users;
"



