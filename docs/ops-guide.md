# FinSentinel 运维部署文档

> 适用版本: v1.0 | 目标读者: 运维工程师 / DevOps | 最后更新: 2025-05

---

## 一、系统架构概述

```
┌────────────────────┐
│ transaction        │  Python 脚本，持续生成模拟交易数据
│ _generator.py      │
└────────┬───────────┘
         │ JSON
         ▼
┌────────────────────┐
│ Kafka Broker       │  Topic: txns (3 partitions)
│ (Confluent 7.5.0)  │
└────────┬───────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│ Spark Structured Streaming (3.5.0)              │
│                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ Raw      │  │ Clean    │  │ Fraud Rules  │  │
│  │ Delta    │  │ Delta    │  │ R1/R2/R3/R4  │  │
│  │ Table    │  │ Table    │  │              │  │
│  └──────────┘  └──────────┘  └──────┬───────┘  │
└─────────────────────────────────────┼───────────┘
                                      │ alerts
                                      ▼
                              ┌──────────────┐
                              │ MongoDB 7.0  │
                              │ fraud_alerts │
                              └──────┬───────┘
                                     │ poll
                                     ▼
                              ┌──────────────┐
                              │ alert_monitor│
                              │ .py (终端)   │
                              └──────────────┘
```

### 1.1 核心组件

| 组件 | 容器名 | 端口 | 用途 |
|------|--------|------|------|
| Zookeeper | zookeeper | 2181 | Kafka 集群协调 |
| Kafka | kafka | 9092 (外部) / 29092 (内部) | 消息队列 |
| MongoDB | mongodb | 27017 | 告警持久化 |
| Spark Master | spark-master | 8080, 7077, 4040 | 流处理引擎 |

### 1.2 组件依赖关系

```
Zookeeper (healthy) → Kafka (healthy) → Spark Master
MongoDB    (healthy) ─────────────────→ Spark Master
```

Spark Master 等待 Kafka 和 MongoDB 二者均健康后才启动，确保流处理作业启动时下游服务已就绪。

---

## 二、环境要求

### 2.1 硬件最低要求

| 资源 | 最低配置 | 推荐配置 |
|------|---------|---------|
| CPU | 4 核 | 8 核 |
| 内存 | 8 GB | 16 GB |
| 磁盘 | 20 GB 可用 | 50 GB SSD |
| 网络 | — | 稳定的互联网连接（拉取镜像） |

### 2.2 软件依赖

| 软件 | 版本要求 | 说明 |
|------|---------|------|
| Docker Desktop | 4.x+ | 容器运行时 |
| Docker Compose | 3.8+ | 编排工具（随 Docker Desktop 附带） |
| Python | 3.10+ | 用于本地运行数据生成器和告警监控 |
| Git | 2.x | 版本管理 |

### 2.3 端口占用检查

部署前确认以下端口未被占用：

```powershell
# Windows PowerShell
netstat -ano | findstr "2181 9092 27017 8080 7077 4040"
```

```bash
# Linux / macOS
lsof -i :2181 -i :9092 -i :27017 -i :8080 -i :7077 -i :4040
```

如果端口冲突，修改 `docker-compose.yml` 中对应的端口映射。

---

## 三、部署指南

### 3.1 首次部署

```bash
# 1. 进入项目目录
cd finsentinel

# 2. 配置环境变量（可选，使用默认值即可）
copy .env.example .env

# 3. 构建并启动所有服务
docker-compose up -d --build

# 4. 等待服务健康检查通过（约 30-60 秒）
docker-compose ps
# 确认所有服务状态为 "healthy" 或 "running"
```

### 3.2 创建 Kafka Topic

首次部署后需要手动创建 Topic：

```bash
docker exec kafka kafka-topics --create \
  --topic txns \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1
```

验证 Topic 创建成功：

```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

### 3.3 安装 Python 依赖

```bash
pip install -r requirements.txt
```

### 3.4 验证部署

```bash
# 1. 检查容器状态
docker-compose ps

# 2. 检查 Spark Master Web UI（浏览器访问）
# http://localhost:8080

# 3. 检查 MongoDB 连接
docker exec mongodb mongosh --eval "db.adminCommand('ping')"

# 4. 检查 Kafka 连接
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

---

## 四、启停流程

### 4.1 启动顺序（重要）

严格按照依赖顺序启动：

```bash
# 1. 启动基础设施
docker-compose up -d zookeeper mongodb
# 等待 zookeeper 和 mongodb 健康

# 2. 启动 Kafka
docker-compose up -d kafka
# 等待 kafka 健康

# 3. 创建 Topic（如未创建）
docker exec kafka kafka-topics --create \
  --topic txns --bootstrap-server kafka:29092 \
  --partitions 3 --replication-factor 1

# 4. 启动 Spark（会自动拉取 Kafka + Mongo 依赖）
docker-compose up -d spark-master

# 5. 在容器内启动流处理作业
docker exec spark-master spark-submit /app/src/spark_streaming.py

# 6. 启动数据生成器（本地终端）
python src/transaction_generator.py

# 7. 启动告警监控（本地终端，可选）
python src/alert_monitor.py
```

### 4.2 一键启动（全栈模式）

```bash
cd finsentinel
docker-compose up -d
```

### 4.3 停止流程

```bash
# 优雅停止（推荐）
docker-compose down

# 停止但保留数据卷（MongoDB 数据不丢失）
docker-compose stop

# 强制停止并清理所有数据
docker-compose down -v
```

### 4.4 重启单个服务

```bash
# 重启 Kafka
docker-compose restart kafka

# 重启 Spark
docker-compose restart spark-master

# 重启 MongoDB
docker-compose restart mongodb
```

---

## 五、服务健康检查

### 5.1 容器级别

Docker Compose 已内置健康检查：

| 服务 | 检查方式 | 检查间隔 | 超时重试 |
|------|---------|---------|---------|
| Zookeeper | `echo srvr \| nc zookeeper 2181` | 10s | 5 |
| Kafka | `kafka-topics --list` | 10s | 5 |
| MongoDB | `mongosh --eval "db.adminCommand('ping')"` | 10s | 5 |
| Spark | 无内建检查 | — | — |

### 5.2 手动检查清单

```bash
# Kafka 连接测试
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092

# MongoDB 连接测试
docker exec mongodb mongosh --eval "db.stats()"

# Spark 作业运行状态
docker exec spark-master bash -c "cat /opt/spark/logs/spark-*.out"

# Kafka 消息积压检查
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 --topic txns
```

### 5.3 Spark Web UI 监控

| 端口 | 用途 | 访问地址 |
|------|------|---------|
| 8080 | Spark Master Web UI | http://localhost:8080 |
| 4040 | Spark 作业 Web UI | http://localhost:4040 |

通过 Web UI 可监控：
- 作业是否在运行（Streaming 标签）
- 批处理耗时和吞吐量
- Executor 资源使用情况
- 错误日志

---

## 六、日志管理

### 6.1 容器日志查看

```bash
# 查看所有服务日志
docker-compose logs -f

# 查看指定服务最近 100 行
docker-compose logs --tail=100 kafka
docker-compose logs --tail=100 spark-master
docker-compose logs --tail=100 mongodb

# 按时间过滤
docker-compose logs --since=10m kafka
```

### 6.2 应用级日志

| 组件 | 日志位置 | 查看方式 |
|------|---------|---------|
| transaction_generator | stdout（终端） | 直接输出到运行终端 |
| spark_streaming | 容器 stdout | `docker logs spark-master -f` |
| alert_monitor | stdout（终端） | 直接输出到运行终端 |
| Delta Lake checkpoint | `data/raw/_checkpoint/` | Spark 自动管理 |
| Kafka broker | 容器 stdout | `docker logs kafka -f` |
| MongoDB | 容器 stdout | `docker logs mongodb -f` |

### 6.3 日志级别调整

**Spark 日志级别** - 修改 SparkSession 配置：

```python
# 在 spark_streaming.py 中降低日志级别
spark.sparkContext.setLogLevel("WARN")  # 可选: ALL, DEBUG, INFO, WARN, ERROR
```

**Kafka 日志** - 通过环境变量在 compose 中设置：

```yaml
environment:
  KAFKA_LOG4J_LOGGERS: "kafka.controller=INFO,state.change.logger=INFO"
```

---

## 七、故障排查

### 7.1 Kafka 未启动/dataDir 冲突

**症状**: Kafka 容器反复重启

```bash
# 查看 Kafka 日志
docker logs kafka

# 常见错误: "Cluster ID mismatch" → dataDir 残留旧数据
docker-compose down -v
docker-compose up -d
```

### 7.2 Spark 无法连接 Kafka

**症状**: Spark 日志中出现 `Failed to construct kafka consumer`

**排查步骤**:
1. 确认 Kafka 容器健康：`docker-compose ps kafka`
2. 确认内部地址正确：环境变量应使用 `kafka:29092`（Docker 内部网络）
3. 在 Spark 容器内测试连接：
   ```bash
   docker exec spark-master bash -c "echo test | nc kafka 29092"
   ```

### 7.3 MongoDB 连接失败

**症状**: 告警未写入 MongoDB

**排查步骤**:
1. 确认 MongoDB 健康：`docker logs mongodb`
2. 确认连接 URI 正确：环境变量应为 `mongodb://mongodb:27017`
3. 手动测试 Mongo 写入：
   ```bash
   docker exec spark-master python -c "
   from pymongo import MongoClient
   c = MongoClient('mongodb://mongodb:27017')
   c.admin.command('ping')
   print('OK')
   "
   ```

### 7.4 Delta Lake 写入失败

**症状**: Spark 日志中出现 `DeltaIOException` 或 `java.io.IOException`

**排查步骤**:
1. 检查 data 目录权限：`ls -la data/`
2. 检查磁盘空间：`df -h`
3. 清理 checkpoint 重启：
   ```bash
   rm -rf data/raw/_checkpoint data/clean/_checkpoint
   ```

### 7.5 内存不足 (OOM)

**症状**: Spark 作业一直处于 `ACCEPTED` 状态但无法运行

**解决方案**:
1. 增加 Docker Desktop 分配内存（Settings → Resources → Memory → 至少 8GB）
2. 或在 `spark-submit` 时限制内存：
   ```bash
   docker exec spark-master spark-submit \
     --conf spark.executor.memory=512m \
     --conf spark.driver.memory=512m \
     /app/src/spark_streaming.py
   ```

### 7.6 数据积压

**症状**: Kafka 中消息堆积，处理延迟增大

**排查步骤**:
```bash
# 检查消费者 Lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group spark-kafka-source-*
```

**缓解措施**:
- 增加 `GENERATOR_BATCH_SIZE` 减少生成频率
- 增加 Kafka 分区数（需重建 Topic）
- 增加 Spark executor 数量

---

## 八、备份与恢复

### 8.1 MongoDB 数据备份

```bash
# 导出告警数据到 JSON
docker exec mongodb mongodump \
  --db fraud_detection \
  --collection fraud_alerts \
  --out /data/backup

# 从容器复制到本地
docker cp mongodb:/data/backup ./mongo_backup_$(Get-Date -Format yyyyMMdd)
```

### 8.2 Delta Lake 数据备份

Delta Lake 数据存储在 `data/` 目录下，直接备份该目录即可：

```bash
# 压缩备份
tar -czf delta_backup_$(date +%Y%m%d).tar.gz data/
```

### 8.3 MongoDB 数据恢复

```bash
# 复制备份到容器
docker cp ./mongo_backup mongodb:/data/restore

# 恢复
docker exec mongodb mongorestore \
  --db fraud_detection \
  --collection fraud_alerts \
  /data/restore/fraud_detection/fraud_alerts.bson
```

### 8.4 配置文件备份

```bash
# 备份关键配置文件
tar -czf config_backup.tar.gz config/ .env docker-compose.yml
```

---

## 九、性能调优

### 9.1 Kafka 性能参数

| 参数 | 当前值 | 建议 | 说明 |
|------|--------|------|------|
| partition | 3 | 3-6 | 提升并行消费能力 |
| replication_factor | 1 | 1-3 | 影响数据可靠性 |

### 9.2 Spark 性能参数

| 参数 | 当前值 | 建议 | 说明 |
|------|--------|------|------|
| watermark | 10 min | 按需调整 | 状态清理窗口，影响内存使用 |
| trigger interval | 默认 (微批) | 可调整为固定间隔 | `.trigger(processingTime="5 seconds")` |
| shuffle partitions | 默认 200 | 4-8 | 小数据量下减少 shuffle 开销 |

建议在 `spark_streaming.py` 中添加：

```python
spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "5")
```

### 9.3 数据生成器调优

在 `.env` 或 `config/settings.py` 中调整：

```env
GENERATOR_BATCH_SIZE=5       # 每秒发送 5 条（默认 1）
GENERATOR_INTERVAL=1         # 间隔秒数
```

### 9.4 资源监控

```bash
# 查看容器资源使用
docker stats

# 查看磁盘使用
docker system df
```

---

## 十、告警处理流程

### 10.1 告警分级

| 规则 | 严重级别 | 响应时间 | 处理建议 |
|------|---------|---------|---------|
| R1 速度欺诈 | 高 | 5 分钟内 | 冻结用户账户，人工审核最近交易 |
| R2 地理跳跃 | 高 | 10 分钟内 | 确认是否被盗刷，联系用户 |
| R3 深夜大额 | 中 | 1 小时内 | 抽查核实交易意图 |
| R4 商户高频 | 中 | 30 分钟内 | 检查商户是否存在批量套现 |

### 10.2 告警查询

```bash
# 在 MongoDB 中查询近期告警
docker exec mongodb mongosh --eval "
  db.fraud_alerts.find().sort({alert_time:-1}).limit(20).pretty()
" fraud_detection

# 按规则类型统计
docker exec mongodb mongosh --eval "
  db.fraud_alerts.aggregate([
    {\$group: {_id: '\$rule_type', count: {\$sum: 1}}}
  ])
" fraud_detection
```

### 10.3 误报处理

如果确认某规则频繁误报，可通过 `config/fraud_rules.yaml` 临时禁用：

```yaml
R1:
  enabled: false  # 禁用该规则
```

或调整阈值：

```yaml
R1:
  threshold: 10   # 从 5 提升到 10
  window_minutes: 10  # 从 5 放宽到 10
```

修改后需重启 Spark 流处理作业。

---

## 十一、升级与变更

### 11.1 代码变更部署流程

```bash
# 1. 停止数据生成器（Ctrl+C）
# 2. 停止 Spark 流处理（Ctrl+C）
# 3. 停止服务
docker-compose down

# 4. 重新构建并启动
docker-compose up -d --build

# 5. 重新创建 Topic（如需要）
docker exec kafka kafka-topics --create ...

# 6. 重新启动所有组件
```

### 11.2 配置热更新

- **规则配置** (`fraud_rules.yaml`): 需重启 Spark 流处理
- **环境变量** (`.env`): 需重启容器
- **Kafka Topic**: 不能在线修改分区数，需重建

### 11.3 回滚步骤

```bash
# 使用 Git 回滚代码
git checkout <previous-commit-hash>

# 清理数据
rm -rf data/raw data/clean

# 重建环境
docker-compose down -v
docker-compose up -d --build
```

---

## 十二、安全注意事项

1. **`.env` 文件不提交到 Git** - 已配置 `.gitignore`
2. **MongoDB 无认证** - 当前为开发模式，生产环境需启用认证
3. **Kafka 无 ACL** - 生产环境需配置 SASL/SSL
4. **Spark Master 无认证** - 端口 8080 和 7077 当前直接暴露
5. **PII 盐值硬编码** - `fraud_rules.py:7` 中 `PII_SALT = "FinSentinel2024"` 应移至环境变量

---

## 十三、常用运维命令速查

```bash
# 查看所有容器状态
docker-compose ps

# 查看资源占用
docker stats

# 进入容器
docker exec -it kafka bash
docker exec -it spark-master bash
docker exec -it mongodb bash

# 查看 Kafka 消息数量
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 --topic txns

# 清空 Kafka Topic
docker exec kafka kafka-topics --delete --topic txns \
  --bootstrap-server kafka:29092

# 查看 Delta 表数据（Spark SQL）
docker exec spark-master spark-sql --packages io.delta:delta-spark_2.12:3.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# 清理 Docker 资源
docker system prune -a --volumes
```
