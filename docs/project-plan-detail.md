# 大数据毕业设计 — 方向 A：实时欺诈检测与风险监控平台

## 项目计划书

---

### 一、项目概述

**项目名称**：FinSentinel — 金融交易实时欺诈检测平台

**一句话定义**：为一家金融科技公司构建基于 Kafka + Spark Structured Streaming 的毫秒级欺诈检测系统，对每笔交易实时判定风险，并将完整数据链路持久化到 Delta Lake 湖仓一体架构中。

**业务场景**：某金融科技独角兽公司（或大型电商平台），全球每分钟发生数百万笔交易。系统必须在交易发生时检测欺诈行为（流处理），通知运营团队，并为每笔交易存储永久的、符合法律规定的记录（湖仓一体），供数据科学团队训练未来模型使用。

**团队成员**：TBD（建议 3-4 人）

- 1 人：Kafka + 数据生成器
- 1 人：Spark Structured Streaming 管道 + 欺诈规则
- 1 人：Delta Lake 存储 + MongoDB 服务层
- 1 人（可选）：Docker 编排 + 测试 + 报告

---

### 二、欺诈检测业务规则

共设计 **4 条启发式规则**，全部在 Spark Structured Streaming 中实现：

| 编号 | 规则名称 | 检测类型 | 触发条件 | 实现方式 |
|------|---------|---------|---------|---------|
| R1 | 速度欺诈 | 有状态窗口 | 同一用户 5 分钟内交易 ≥ 5 笔 | 滑动窗口 count(UserID) |
| R2 | 地理跳跃 | 流-流关联 | 同一用户前后两笔交易地理位置距离 > 1000km 且间隔 < 10 分钟 | 左外连接当前批与上一批 |
| R3 | 深夜大额 | 无状态过滤 | 交易金额 > $5000 且时间在 00:00-05:00 | 简单 filter |
| R4 | 商户高频 | 有状态窗口 | 同一商户 1 分钟内交易 > 3 笔（防止刷单） | 滑动窗口 count(MerchantID) |

**规则可扩展点**：

- 规则配置外置在 `fraud_rules.yaml` 中，阈值可动态调整
- 预留黑名单（用户/商户/IP）静态筛选接口
- 符合 A 级标准：至少一个 stateful 操作（R1/R4 的窗口聚合）

---

### 三、系统架构

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

**数据流路径（一笔交易的生命周期）**：

1. `transaction_generator.py` 生成 JSON 交易 →
2. Kafka Topic `txns`（3 分区）→
3. Spark Structured Streaming 消费 → 并行分叉：
   - 分支 A：原样写入 `Raw Delta Table`
   - 分支 B：PII 脱敏后写入 `Clean Delta Table`
   - 分支 C：应用 4 条欺诈规则，命中则写入 MongoDB `fraud_alerts`
4. `alert_monitor.py` 轮询 MongoDB → 终端实时打印

---

### 四、技术栈选型与权衡论证

| 层次 | 技术 | 替代方案 | 选择理由 |
|------|------|---------|---------|
| 消息队列 | Apache Kafka | RabbitMQ / Pulsar | 课程要求；高吞吐、分区有序、天然适配 Spark 连接器 |
| 流处理引擎 | Spark Structured Streaming | Flink / Kafka Streams | 课程要求；与 Delta Lake 同源生态，DataFrame API 统一批流 |
| 湖仓存储 | Delta Lake (OSS) | Iceberg / Hudi | Spark 原生集成 ACID 事务、时间旅行；指定要求 Raw+Clean 双表 |
| 告警存储 | MongoDB | PostgreSQL / Redis | 文档模型适配半结构化告警（规则命中 + 元数据）；查询灵活无需固定 Schema |
| 编排 | Docker Compose | k8s / 手动 | 一键启动全栈（Kafka + Spark + Mongo），可复现，教学演示友好 |
| 语言 | Python 3.10+ | Scala | 团队大多数熟悉 Python；PySpark API 完全覆盖需求 |

**为什么不选 Pandas？** Pandas 是单机内存处理，无法处理持续流数据，没有状态管理和窗口语义。Spark Structured Streaming 有 Exactly-Once 语义保证，原生支持滑动窗口、水印、迟到数据处理。

**为什么不选 PostgreSQL？** 告警数据字段多变（不同规则命中携带不同上下文），MongoDB 的 Document 模型天然适配。PostgreSQL 适合固定 Schema 的聚合表（Gold 层），但这里不是。

---

### 五、数据模型与 Schema

#### 5.1 交易输入 Schema（Kafka / JSON）

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

#### 5.2 Raw Delta Table（原始层）

| 字段 | 类型 | 说明 |
|------|------|------|
| transaction_id | String | 交易唯一标识 |
| user_id | String | 原始用户 ID |
| merchant_id | String | 商户 ID |
| amount | Double | 金额 |
| timestamp | Timestamp | 交易时间 |
| latitude | Double | GPS 纬度 |
| longitude | Double | GPS 经度 |
| ip_address | String | 原始 IP |
| device_id | String | 设备 ID |
| transaction_type | String | 交易类型 |
| kafka_partition | Int | 来源分区 |
| kafka_offset | Long | 来源位移 |
| ingest_time | Timestamp | 接入时间 |

#### 5.3 Clean Delta Table（清洗脱敏层）

与 Raw 表相同，但：

- `user_id` → `user_hash`（SHA256 哈希 + 盐值）
- `ip_address` → `ip_mask`（只保留前两段，如 `192.168.*.*`）
- 移除 `device_id` 字段

#### 5.4 MongoDB — fraud_alerts Collection

```json
{
  "alert_id": "ALERT-speed-20250512-001",
  "rule_type": "VELOCITY_FRAUD",
  "transaction_id": "TXN-20250512-001",
  "user_hash": "a1b2c3...",
  "amount": 120.50,
  "timestamp": "2025-05-12T14:30:00Z",
  "trigger_detail": {
    "rule": "R1: 5 transactions in 5 minutes",
    "window_start": "2025-05-12T14:25:00Z",
    "window_end": "2025-05-12T14:30:00Z",
    "txn_count": 5,
    "threshold": 4
  },
  "alert_time": "2025-05-12T14:30:02Z"
}
```

---

### 六、模块划分与代码结构

```
finsentinel/
│
├── docker-compose.yml          # Kafka + Zookeeper + Spark + MongoDB 全栈编排
├── Dockerfile                  # 自定义 Spark Master 镜像（含项目代码）
├── requirements.txt            # Python 依赖
├── README.md                   # 完整部署运行指南
│
├── config/
│   ├── __init__.py
│   ├── settings.py             # 统一配置（环境变量读取，无硬编码路径）
│   └── fraud_rules.yaml        # 欺诈规则阈值配置
│
├── src/
│   ├── __init__.py
│   ├── schemas.py              # 所有 Spark Schema 定义
│   ├── transaction_generator.py  # 交易数据生成 & 推送到 Kafka
│   ├── spark_streaming.py      # Spark Structured Streaming 主作业
│   ├── fraud_rules.py          # 欺诈检测规则函数集
│   └── alert_monitor.py        # MongoDB 告警实时监听终端输出
│
├── notebooks/
│   └── pipeline_demo.ipynb     # Jupyter Notebook 完整流程演示
│
├── tests/
│   ├── test_fraud_rules.py     # 欺诈规则单元测试
│   └── test_schema.py          # Schema 验证测试
│
└── data/                       # (gitignored) Delta Lake 本地存储路径
    ├── raw/
    └── clean/
```

**模块职责**：

| 模块 | 输入 | 输出 | 关键函数 |
|------|------|------|---------|
| `settings.py` | 环境变量 | Kafka/Mongo/Delta 连接参数 | `get_config()` |
| `schemas.py` | — | StructType 定义 | `transaction_schema`, `alert_schema` |
| `transaction_generator.py` | 欺诈注入标记 | Kafka topic | `generate_normal_tx()`, `inject_fraud_tx()` |
| `spark_streaming.py` | Kafka 流 | Delta + MongoDB | `main_streaming_pipeline()` |
| `fraud_rules.py` | DataFrame 微批 | 告警 DataFrame | `check_velocity()`, `check_geo_jump()`, 等 |
| `alert_monitor.py` | MongoDB change stream | 终端输出 | `watch_alerts()` |

---

### 七、实施路线图（Sprint 计划）

#### Sprint 0：环境搭建（第 1-2 天）

- [ ] 安装 Docker Desktop（全员）
- [ ] 创建 `docker-compose.yml`，启动 Kafka + Zookeeper + MongoDB
- [ ] 安装 `pyspark`, `delta-spark`, `pymongo`, `kafka-python`
- [ ] 验证：能通过 `localhost:9092` 创建 topic 并收发消息

#### Sprint 1：行走骨架（第 3-4 天）★ 最关键

- [ ] 实现 `transaction_generator.py`，每秒生成 1 笔 JSON 写入 Kafka
- [ ] 实现 `spark_streaming.py` 最简版：读取 Kafka → 打印到控制台
- [ ] 实现数据写入 Raw Delta Table
- [ ] **验证**：模拟数据能贯穿整个管道（Generator → Kafka → Spark → Delta）

#### Sprint 2：规则引擎（第 5-7 天）

- [ ] 实现 R3 深夜大额（最简单，纯过滤）
- [ ] 实现 R1 速度欺诈（滑动窗口，有状态）
- [ ] 实现 R2 地理跳跃（流内连接）
- [ ] 实现 R4 商户高频（滑动窗口）
- [ ] 告警写入 MongoDB（每条规则命中写 1 条）
- [ ] **验证**：注入坏交易，MongoDB 出现对应告警

#### Sprint 3：脱敏 + 完善（第 8-10 天）

- [ ] 实现 Clean Delta Table 写入（PII 哈希脱敏）
- [ ] 实现 `alert_monitor.py` 终端监听
- [ ] 编写 `fraud_rules.yaml` 外部配置
- [ ] 增加水印机制（处理迟到数据）
- [ ] 异常处理（Kafka 断连重试、MongoDB 写入重试）

#### Sprint 4：测试 + 文档（第 11-14 天）

- [ ] 单元测试：欺诈规则逻辑正确性
- [ ] 集成测试：端到端 3 分钟完整流程
- [ ] Jupyter Notebook 演示（pipeline_demo.ipynb）
- [ ] 编写设计报告（架构图、技术权衡、云成本估算、事后剖析）
- [ ] 预录制演示视频（防 demo god 翻车）

---

### 八、Docker Compose 服务规划

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    ports: [2181]
    environment: ...

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports: [9092]
    depends_on: [zookeeper]
    environment: ...

  mongodb:
    image: mongo:7.0
    ports: [27017]

  spark-master:
    build: .
    ports: [8080, 7077, 4040]
    volumes: [./src:/app/src, ./notebooks:/app/notebooks]
    command: /opt/spark/sbin/start-master.sh && sleep infinity
```

**不选 Kubernetes 的理由**：3 人团队、单机部署、教学演示场景。Docker Compose 启动一条命令，评分老师零配置可复现——这是 MD 文件明确要求的。

---

### 九、云成本估算

假设部署在阿里云上，处理每日 10TB 交易数据：

| 资源 | 规格 | 月费（估算） |
|------|------|------------|
| Kafka（3 broker, 16C32G × 3） | 阿里云 Kafka 专业版 | ¥8,000 × 3 = ¥24,000 |
| Spark（EMR, 10 worker, 8C16G） | EMR Spark 集群 | ¥15,000 |
| Delta Lake 存储（OSS） | 10TB × 30天 × 3副本 + 冗余 | ¥20,000 |
| MongoDB（副本集, 3节点, 4C8G） | MongoDB 云数据库 | ¥6,000 |
| 网络 + 公网带宽 | — | ¥3,000 |
| **合计** | | **约 ¥68,000/月** |

> 注：此估算为架构设计参考值。实际生产还需考虑监控告警（Grafana）、日志（ELK）、备份策略等。

---

### 十、测试与验证方案

#### 10.1 单元测试

- `test_fraud_rules.py`：构造已知结果的交易输入，断言每条规则输出是否正确
  - R3：金额 $6000 + 凌晨 3:00 → 应命中
  - R3：金额 $6000 + 下午 2:00 → 不命中
  - R1：模拟 5 分钟内 6 笔交易 → 应命中

#### 10.2 集成测试（教师视角验证）

执行以下步骤，总计 < 3 分钟：

```bash
# 1. 一键启动全栈
docker-compose up -d

# 2. 等待 Kafka 就绪，创建 topic
# 3. 提交 Spark 作业
docker exec spark-master spark-submit src/spark_streaming.py &

# 4. 启动数据生成器（正常模式）
python src/transaction_generator.py

# 5. 启动告警监控
python src/alert_monitor.py

# 6. 注入一笔欺诈交易（触发 R3）
python src/transaction_generator.py --inject-fraud R3

# 7. 验证：alert_monitor.py 终端应在 3 秒内打印红字告警
# 8. 验证：Delta Lake 存储路径下 Raw 和 Clean 表均有新数据
```

#### 10.3 A 级标准验证清单

```
□ docker-compose up 一键启动成功
□ Generator 持续产生 JSON 交易数据写入 Kafka
□ Spark Structured Streaming 正常消费 Topic
□ 实现至少一个有状态操作（R1 滑动窗口/或 R4 滑动窗口）
□ 原始事件写入 Raw Delta Table
□ PII 脱敏版本写入 Clean Delta Table (user_id 哈希, IP 掩码)
□ 注入坏交易 → 3 秒内 MongoDB 告警出现
□ alert_monitor.py 终端实时打印告警
□ 无硬编码路径（全部走环境变量）
□ 提供 requirements.txt
□ 提供 docker-compose.yml
□ 提供 Jupyter Notebook 演示
□ 代码模块化（非上帝脚本）
```

---

### 十一、演示脚本（10 分钟 + 5 分钟 Q&A）

| 时间段 | 内容 | 负责人 |
|--------|------|--------|
| 0:00-1:00 | 快速介绍：业务场景 + 系统目标 | 全员选择 |
| 1:00-2:30 | 架构图讲解：为什么选这些技术 | 架构负责人 |
| 2:30-5:00 | **现场演示**（录像备份）：docker-compose up → 注入坏交易 → 告警秒级出现 | 开发负责人 |
| 5:00-7:00 | 代码走读：Spark 有状态操作、Delta 分层、MongoDB 写入 | 开发负责人 |
| 7:00-8:30 | 技术权衡：Spark vs Flink, Mongo vs PG, 水印 vs 无状态 | 架构负责人 |
| 8:30-10:00 | 管理视角：云成本估算、事后剖析、已知限制 | 项目经理 |
| 10:00-15:00 | Q&A："如果节点故障？""迟到数据怎么处理？""如何扩展到 100 万 TPS？" | 全员 |

**答辩准备要点**（教师会问的问题）：

- "这段窗口聚合代码怎么处理锁和水印？" → 准备好 Spark 源码级解释
- "如果 Kafka 挂了你怎么办？" → Spark 自带 checkPointing 自动恢复位移
- "如果一条数据迟到了 30 分钟？" → 水印机制 + late arrival 策略
- "为什么不用 Flink？" → 准备对比论述
- "你写的哪段代码？" → 每人必须能指认自己的代码并解释

---

### 十二、常见失败模式预防

| 陷阱 | 预防措施 |
|------|---------|
| UI 陷阱 | 不做任何前端。`alert_monitor.py` 只在终端输出红色文字，满足"操作控制台"要求 |
| 上帝脚本 | 严格按目录结构分模块，每个文件 < 200 行 |
| 硬编码路径 | `settings.py` 统一管理，全部从环境变量读取，提供 `.env.example` |
| 集成地狱 | Sprint 1 第 3 天就完成端到端走骨架，绝不等到最后一周 |
| HDFS 依赖 | 使用本地文件系统模式 `delta_path = ./data/raw`，不引入 HDFS |
| Window 路径 | Windows 用户 `delta_path` 用相对路径 `./data/raw`，统一在 Docker 内运行 |

---

### 十三、风险管理

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| Docker Desktop 安装失败 | 中 | 高 | 单人 Docker + 多人远程连接；备选方案：本地 Python 直连进程 |
| Spark + Kafka 版本兼容 | 中 | 中 | 使用已验证的 Confluent 7.5 + Spark 3.5 组合 |
| Spark Structured Streaming 在 Windows 上表现不稳定 | 中 | 中 | 全流程在 Docker Linux 容器内运行 |
| 队友零贡献 | 低 | 高 | 代码仓库每条 commit 可追溯作者；同行评议机制威慑 |

---

### 十四、团队分工模板

| 角色 | GitHub 账号 | 负责模块 | 工作量 |
|------|------------|---------|--------|
| 数据管道工程师 | @alice | `transaction_generator.py` + Kafka 配置 | 25% |
| 流处理工程师 | @bob | `spark_streaming.py` + `fraud_rules.py` + `schemas.py` | 35% |
| 存储/服务工程师 | @charlie | Delta Lake 配置 + MongoDB + `alert_monitor.py` + Docker | 30% |
| 文档/演示负责人 | @shared | `pipeline_demo.ipynb` + 设计报告 + 演示视频 | 10% |
