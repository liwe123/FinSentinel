# FinSentinel — 金融交易实时欺诈检测与风险监控平台

> 基于 Kafka + Spark Structured Streaming + Delta Lake 的毫秒级欺诈检测系统

---

## 一、项目概述

FinSentinel 为金融科技公司构建的实时欺诈检测平台。系统对每笔交易实时判定风险（流处理），通知运营团队（告警），并为每笔交易存储永久的、符合法律规定的记录（湖仓一体），供数据科学团队训练未来模型使用。

**核心能力**:
- 毫秒级欺诈检测（4 条启发式规则）
- 原始数据湖仓落地（Raw + Clean Delta Tables）
- PII 数据自动脱敏（SHA256 哈希 + IP 掩码）
- 实时告警推送（MongoDB + 终端监控）

---

## 二、系统架构

```
                       ┌──────────────────────────────────────────────────────────┐
                       │                   Apache Spark (Master)                   │
                       │                                                          │
                       │  ┌─────────────────────────────────────────────────────┐ │
  ┌─────────────────┐   │  │        Spark Structured Streaming Job              │ │
  │  transaction     │   │  │                                                     │ │
  │  _generator.py   │───▶│  │  ┌──────────┐    ┌──────────────┐    ┌─────────┐  │ │
  │                  │   │  │  │  Kafka   │───▶│  Fraud Rules │───▶│  Alerts │  │ │
  │  持续产生模拟    │   │  │  │  Source  │    │  Engine      │    │  Sink   │  │ │
  │  JSON 交易数据   │   │  │  └──────────┘    │  (R1-R4)     │    └────┬────┘  │ │
  └────────┬─────────┘   │  │                  └──────┬───────┘         │        │ │
           │             │  │                         │                 │        │ │
           ▼             │  │                ┌────────┴────────┐        │        │ │
  ┌─────────────────┐    │  │                ▼                 ▼        ▼        │ │
  │   Kafka Broker  │    │  │  ┌────────────────────┐ ┌────────────────────┐     │ │
  │                 │    │  │  │  Raw Delta Table   │ │ Clean Delta Table  │     │ │
  │  Topic: txns    │    │  │  │  (原始明文)        │ │ (PII 哈希脱敏)     │     │ │
  │  3 partitions   │    │  │  └────────────────────┘ └────────────────────┘     │ │
  └─────────────────┘    │  └─────────────────────────────────────────────────────┘ │
                         └──────────────────────────────────────────────────────────┘
                                                              │
                                                              │ fraud alerts
                                                              ▼
                                                  ┌────────────────────┐
                                                  │     MongoDB        │
                                                  │  Collection:       │
                                                  │  fraud_alerts      │
                                                  └─────────┬──────────┘
                                                            │
                                                            ▼
                                                  ┌────────────────────┐
                                                  │  alert_monitor.py  │
                                                  │  终端实时输出告警   │
                                                  └────────────────────┘
```

**数据流路径**:
1. `transaction_generator.py` → JSON 交易数据
2. Kafka Topic `txns`（3 分区）→
3. Spark Structured Streaming 消费 → 三路并行:
   - **分支 A**: 写入 Raw Delta Table
   - **分支 B**: PII 脱敏后写入 Clean Delta Table
   - **分支 C**: 应用 4 条欺诈规则 → MongoDB `fraud_alerts`
4. `alert_monitor.py` 实时打印告警

---

## 三、快速启动

### 环境要求

- Docker Desktop 4.x+
- Python 3.10+
- 8GB+ 可用内存

### 1. 一键启动全栈

```bash
# 在项目根目录
cd finsentinel
docker-compose up -d

# 等待所有服务就绪（约 30 秒）
docker-compose ps
```

### 2. 安装 Python 依赖

```bash
pip install -r requirements.txt
```

### 3. 创建 Kafka Topic（首次启动后）

```bash
docker exec kafka kafka-topics --create \
  --topic txns \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1
```

### 4. 启动 Spark 流处理

```bash
docker exec spark-master spark-submit /app/src/spark_streaming.py
```

### 5. 启动数据生成器（另一个终端）

```bash
# 正常模式：持续生成交易数据
python src/transaction_generator.py
```

### 6. 启动告警监控（第三个终端）

```bash
python src/alert_monitor.py
```

### 7. 注入欺诈交易（第四个终端）

```bash
# 注入 R1 速度欺诈交易
python src/transaction_generator.py --inject-fraud R1

# 注入 R3 深夜大额交易
python src/transaction_generator.py --inject-fraud R3
```

**验证成功标志**: `alert_monitor.py` 终端应在 3 秒内打印红色告警信息。

---

## 四、欺诈检测规则

| 编号 | 规则名称 | 检测类型 | 触发条件 | 实现方式 |
|------|---------|---------|---------|---------|
| R1 | 速度欺诈 | 有状态窗口 | 同一用户 5 分钟内交易 ≥ 5 笔 | 滑动窗口 count(UserID) |
| R2 | 地理跳跃 | 流内关联 | 同一用户前后两笔交易地理位置距离 > 1000km 且间隔 < 10 分钟 | Window + lag() |
| R3 | 深夜大额 | 无状态过滤 | 交易金额 > $5000 且时间在 00:00-05:00 | 简单 filter |
| R4 | 商户高频 | 有状态窗口 | 同一商户 1 分钟内交易 > 3 笔 | 滑动窗口 count(MerchantID) |

规则阈值通过 `config/fraud_rules.yaml` 外置配置，可动态调整。

---

## 五、项目结构

```
finsentinel/
├── docker-compose.yml          # Kafka + Zookeeper + Spark + MongoDB 全栈编排
├── Dockerfile                  # 自定义 Spark Master 镜像
├── requirements.txt            # Python 依赖
├── README.md                   # 本文件
├── .env.example               # 环境变量模板
│
├── config/
│   ├── __init__.py
│   ├── settings.py             # 统一配置（环境变量读取）
│   └── fraud_rules.yaml        # 欺诈规则阈值配置
│
├── src/
│   ├── __init__.py
│   ├── schemas.py              # 所有 Spark Schema 定义
│   ├── transaction_generator.py  # 交易数据生成 & 推送 Kafka
│   ├── spark_streaming.py      # Spark Structured Streaming 主作业
│   ├── fraud_rules.py          # 欺诈检测规则函数集
│   └── alert_monitor.py        # MongoDB 告警实时监听
│
├── notebooks/
│   └── pipeline_demo.ipynb     # Jupyter Notebook 完整流程演示
│
├── tests/
│   ├── __init__.py
│   ├── test_fraud_rules.py     # 欺诈规则单元测试
│   └── test_schema.py          # Schema 验证测试
│
└── data/                       # (gitignored) Delta Lake 本地存储
    ├── raw/
    └── clean/
```

---

## 六、技术栈

| 层次 | 技术 | 版本 |
|------|------|------|
| 消息队列 | Apache Kafka (Confluent) | 7.5.0 |
| 流处理引擎 | Apache Spark Structured Streaming | 3.5.0 |
| 湖仓存储 | Delta Lake (OSS) | 3.0.0 |
| 告警存储 | MongoDB | 7.0 |
| 编排 | Docker Compose | 3.8 |
| 语言 | Python | 3.10+ |

**技术选型理由**:
- **Spark vs Pandas**: Pandas 是单机内存处理，无法处理持续流数据；Spark Structured Streaming 有 Exactly-Once 语义，原生支持滑动窗口、水印
- **MongoDB vs PostgreSQL**: 告警字段多变（不同规则携带不同上下文），Document 模型天然适配
- **Delta Lake vs 其他**: Spark 原生集成 ACID 事务、时间旅行、Schema 演进

---

## 七、测试

```bash
# 确保在 Docker 容器内运行（Spark 环境）
docker exec spark-master bash -c "cd /app && pip install pytest && pytest tests/ -v"

# 或在本地 Spark 环境下
pytest tests/ -v
```

**测试覆盖**:
- R1~R4 每条规则的命中和不命中场景
- Schema 字段完整性和可空性验证

---

## 八、数据模型

### 交易输入 (Kafka JSON)

```json
{
  "transaction_id": "TXN-20250512-001",
  "user_id": "U1001",
  "merchant_id": "M3001",
  "amount": 120.50,
  "currency": "USD",
  "timestamp": "2025-05-12T14:30:00Z",
  "location": { "latitude": 40.7128, "longitude": -74.0060, "city": "New York" },
  "ip_address": "192.168.1.100",
  "device_id": "DEV-abc123",
  "transaction_type": "purchase"
}
```

### Raw Delta Table（原始层）

完整的交易记录加上 `kafka_partition`, `kafka_offset`, `ingest_time` 元数据。

### Clean Delta Table（脱敏层）

- `user_id` → `user_hash` (SHA256 + 盐值)
- `ip_address` → `ip_mask` (只保留前两段)
- 移除 `device_id`

### MongoDB fraud_alerts 告警

```json
{
  "alert_id": "ALERT-NIGHT_LARGE-20250512143002",
  "rule_type": "NIGHT_LARGE",
  "transaction_id": "TXN-20250512-001",
  "user_hash": "a1b2c3...",
  "amount": 6000.00,
  "timestamp": "2025-05-12T03:00:00Z",
  "trigger_detail": { "rule": "R3: 深夜大额", ... },
  "alert_time": "2025-05-12T14:30:02Z"
}
```

---

## 九、云成本估算（阿里云）

| 资源 | 规格 | 月费 |
|------|------|------|
| Kafka（3 broker） | 16C32G × 3 | ¥24,000 |
| Spark EMR（10 worker） | 8C16G × 10 | ¥15,000 |
| Delta Lake 存储（OSS） | 10TB × 30天 | ¥20,000 |
| MongoDB 副本集（3节点） | 4C8G × 3 | ¥6,000 |
| 网络 + 公网带宽 | — | ¥3,000 |
| **合计** | | **¥68,000/月** |

---

## 十、环境变量

复制 `.env.example` 为 `.env` 并根据需要修改:

| 变量 | 默认值 | 说明 |
|------|--------|------|
| KAFKA_BOOTSTRAP_SERVERS | localhost:9092 | Kafka 地址 |
| KAFKA_TOPIC | txns | Kafka 主题 |
| MONGO_URI | mongodb://localhost:27017 | MongoDB 连接 |
| DELTA_RAW_PATH | ./data/raw | Raw 表路径 |
| DELTA_CLEAN_PATH | ./data/clean | Clean 表路径 |

---

## 十一、已知限制与事后剖析

1. **Docker Desktop 依赖**: 单机部署方案，生产环境应迁移至 Kubernetes
2. **规则启发式**: 当前欺诈规则为固定阈值，未来应引入 ML 模型
3. **无前端界面**: 告警仅通过终端输出，符合"不做 UI"要求
4. **本地文件系统**: 使用 Docker 本地挂载，非分布式存储
