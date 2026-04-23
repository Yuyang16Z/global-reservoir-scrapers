# MWR OCR 部署到 Oracle Cloud Free Tier

步骤按顺序做，大概 30 分钟可以跑通。

## 1. 创建 Oracle VM

登录 https://cloud.oracle.com → 选一个离你近的 Region（建议 **US West (San Jose)** 或 **Japan East (Tokyo)**；中国大陆访问 MWR 页面从日本区延迟更低）。

### 1.1 Compute → Instances → Create instance

| 字段 | 选择 |
|---|---|
| **Name** | `mwr-ocr-vm` |
| **Placement** | 默认 availability domain |
| **Image** | Change image → **Canonical Ubuntu 24.04** (Operating system: Ubuntu, Image build: minimal)，**Architecture: Aarch64** |
| **Shape** | Change shape → Virtual machine → **Ampere → VM.Standard.A1.Flex** → OCPUs=`2`, Memory=`12 GB`（这是 Always Free 额度内的配置） |
| **Primary VNIC** | 默认 VCN（如果没有就 create new VCN），**Assign a public IPv4 address ✅** 勾上 |
| **Add SSH keys** | **Paste public keys** → 粘下面这行： |

```
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIE9GUbh29QTyEdoqT0Oj6ni2+DG91tQbHj+Bh2yqVANF andyzeng666666@gmail.com oracle-mwr-vm
```

| **Boot volume** | 默认（50 GB 就够，每天 ~10 MB，截图不提交到 repo） |

点 **Create**。等 1-2 分钟 instance 变绿 (RUNNING)。

### 1.2 记下公网 IP

Instance 详情页右上角 **Public IP address**，复制下来，下面假设是 `<VM_IP>`。

### 1.3 在 VCN 里开放 22 端口（已默认开）

默认 Always Free VCN 的 default security list 已经放行 22/TCP。如果用自建 VCN 要手动加：
- VCN → Security Lists → Default → Ingress → Add Ingress Rule：
  Source CIDR `0.0.0.0/0`，Protocol TCP，Dest Port `22`。

（这个脚本不对外开任何端口，只需要 22 让你 SSH 进来。）

---

## 2. SSH 进 VM，初始化环境

Mac 本地终端：

```bash
ssh -i ~/.ssh/oracle/oracle_mwr ubuntu@<VM_IP>
```

首次登录确认指纹，输 `yes`。

进到 VM 后运行：

```bash
# 先 clone repo（用 https，等下再换成 SSH deploy key）
git clone https://github.com/Yuyang16Z/global-reservoir-scrapers.git
cd global-reservoir-scrapers

# 装系统包 + venv + Python 依赖
sudo bash scrapers/china/mwr/setup_oracle_vm.sh
```

装完成大概 5-10 分钟（PaddleOCR + paddlepaddle 包比较大）。

---

## 3. 配置 GitHub Deploy Key

### 3.1 在 GitHub 上添加公钥

1. 打开 https://github.com/Yuyang16Z/global-reservoir-scrapers/settings/keys
2. 点 **Add deploy key**
3. Title: `oracle-mwr-vm`
4. Key: 粘下面这行（**注意这是 public key，不是 private**）：

```
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDJ+CMTqcBgYBwq6s952Ubz0O28gv9UQRU4jVH7pffsJ github-deploy-key global-reservoir-scrapers (oracle mwr VM)
```

5. ✅ **Allow write access**（必须勾上，否则 cron 推不了）
6. Add key

### 3.2 把私钥传到 VM

Mac 本地开一个新终端：

```bash
scp -i ~/.ssh/oracle/oracle_mwr \
    ~/.ssh/oracle/github_deploy_key \
    ubuntu@<VM_IP>:~/.ssh/github_deploy_key
```

### 3.3 在 VM 上配置 git

回到 VM 的 SSH 会话：

```bash
cd ~/global-reservoir-scrapers
bash scrapers/china/mwr/setup_git_deploy_key.sh
```

脚本会：
- 给 deploy key 配 SSH config + known_hosts
- 测试 `ssh -T git@github.com-mwr` 看能不能连
- 把 repo 的 `origin` 切成 SSH URL，`git pull --rebase` 一次

看到 `SSH auth OK` + `git pull` 成功就行了。

---

## 4. 手动跑一次冒烟测试

```bash
cd ~/global-reservoir-scrapers
OUTPUT_DIR="$PWD/data/china/mwr" \
MWR_HEADLESS=1 \
~/venvs/mwr/bin/python scrapers/china/mwr/grab_data.py
```

第一次会下载 PaddleOCR 模型（~300 MB，缓存在 `~/.paddleocr/`），之后就快了。

跑完检查：

```bash
ls data/china/mwr/$(date -u +%F)/
# 应该看到 mwr_ocr_table_YYYY-MM-DD.csv, mwr_ocr_screens_*.csv, screens/, ocr_json/, ocr_txt/
```

---

## 5. 挂定时任务（cron）

```bash
crontab -e
```

加一行（每天 UTC 12:00 = 北京 20:00，跟你 Mac 上的 launchd 时间一致）：

```cron
0 12 * * * /bin/bash -lc '$HOME/global-reservoir-scrapers/scrapers/china/mwr/run_and_commit.sh'
```

---

## 6. 停掉 Mac 上的 launchd

等 VM 上 cron 跑通一次之后（明天 20:00 看 `data/china/mwr/run_logs/` 有没有新文件），在 Mac 上：

```bash
launchctl unload ~/Library/LaunchAgents/com.mwr.ocr.plist
mv ~/Library/LaunchAgents/com.mwr.ocr.plist ~/Library/LaunchAgents/com.mwr.ocr.plist.disabled
```

这样 Mac 就不会再抢跑了。VM 和 Mac 同时跑会在 git 上打架。

---

## 故障排查

- **SSH 连不上 VM**：检查 security list 有没有放 22/TCP；`ping <VM_IP>` 看网络
- **`chromedriver` 版本和 `chromium-browser` 不匹配**：`apt` 里的 chromium 是 snap 版，两个一起升级没问题。如果报错 `SessionNotCreatedException`，`sudo snap refresh chromium`
- **`paddlepaddle` aarch64 wheel 找不到**：`pip install paddlepaddle -i https://www.paddlepaddle.org.cn/packages/stable/cpu/` 用官方镜像
- **cron 跑但不提交**：看 `data/china/mwr/run_logs/cron_*.log`
- **`ssh -T github.com-mwr` 报 Permission denied**：检查 deploy key 是否勾了 "Allow write access"
