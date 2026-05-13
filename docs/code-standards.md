# FinSentinel 代码规范性文档

> 适用版本: v1.0 | 目标读者: 开发者 | 最后更新: 2025-05

---

## 一、项目结构规范

### 1.1 目录结构

```
finsentinel/
├── docker-compose.yml          # 容器编排（服务拓扑的关系在此）
├── Dockerfile                  # 生产级镜像定义
├── requirements.txt            # 全局 Python 依赖（无版本范围符号 ^/~）
├── .env.example               # 环境变量模板（不含密钥）
├── .gitignore                  # Git 忽略规则
│
├── config/                     # 配置层 — 纯粹的数据，无业务逻辑
│   ├── __init__.py
│   ├── settings.py             # 从环境变量读取配置
│   └── fraud_rules.yaml        # 外部化规则配置
│
├── src/                        # 源代码 — 每个模块单一职责
│   ├── __init__.py
│   ├── schemas.py              # 仅定义 Spark Schema
│   ├── transaction_generator.py
│   ├── spark_streaming.py      # 主入口，编排各模块
│   ├── fraud_rules.py          # 规则引擎：函数集
│   ├── delta_writer.py         # Delta Lake 写入逻辑
│   ├── mongo_writer.py         # MongoDB 写入逻辑
│   └── alert_monitor.py        # 告警展示
│
├── tests/                      # 测试与 src/ 一一对应
│   ├── __init__.py
│   ├── test_fraud_rules.py     # 测试 fraud_rules.py
│   └── test_schema.py          # 测试 schemas.py
│
├── notebooks/                  # 探索性/演示性质的 Notebook
│   └── pipeline_demo.ipynb
│
└── data/                       # gitignored — 运行时产生的数据
    ├── raw/
    └── clean/
```

### 1.2 模块组织原则

| 规则 | 说明 | 检查方式 |
|------|------|---------|
| **单一职责** | 每个 `.py` 文件只做一件事 | 文件名即模块职责描述 |
| **无上帝脚本** | 单文件不超过 **200 行** | `wc -l src/*.py` |
| **无业务逻辑在 config/** | config 只放静态配置，不放函数 | 人工 Review |
| **无硬编码路径/密钥** | 所有可变值走环境变量或 YAML | `grep -r "mongodb://" src/` |
| **src/ 不 import tests/** | 源码不依赖测试代码 | — |

### 1.3 当前符合度

| 文件 | 行数 | 职责数 | 状态 |
|------|------|--------|------|
| `schemas.py` | 63 | 1 | ✓ |
| `fraud_rules.py` | 132 | 1 | ✓ |
| `delta_writer.py` | 66 | 1 | ✓ |
| `mongo_writer.py` | 115 | 1 (类 + 工具函数) | ✓ |
| `alert_monitor.py` | 67 | 1 | ✓ |
| `transaction_generator.py` | 133 | 1 | ✓ |
| `spark_streaming.py` | 91 | 1 (编排) | ✓ |
| `settings.py` | 28 | 1 | ✓ |

所有模块均在 200 行以内，职责单一，符合规范。

---

## 二、命名规范

### 2.1 Python 命名

| 类型 | 规范 | 示例 | 检查 |
|------|------|------|------|
| 模块文件 | `snake_case` | `transaction_generator.py` | ✓ 全项目一致 |
| 类名 | `PascalCase` | `MongoAlertWriter`, `AlertMonitor` | ✓ |
| 函数名 | `snake_case` | `check_velocity()`, `generate_normal_tx()` | ✓ |
| 变量名 | `snake_case` | `bootstrap_servers`, `batch_df` | ✓ |
| 常量 | `SCREAMING_SNAKE_CASE` | `PII_SALT`, `MERCHANTS`, `USERS` | ✓ |
| 私有函数/变量 | 前缀 `_` | `_insert_with_retry()`, `_build_trigger_detail()` | ✓ |
| 布尔变量 | `is_` / `has_` 前缀 | — | — (暂无布尔变量命名场景) |
| 测试函数 | `test_<模块>_<场景>` | `test_check_night_large_hit` | ✓ |

### 2.2 环境变量

| 规范 | 示例 |
|------|------|
| `UPPER_SNAKE_CASE`，层级前缀 | `KAFKA_BOOTSTRAP_SERVERS`, `MONGO_URI` |
| `.env.example` 不含密钥 | ✓ |

### 2.3 数据库/集合命名

| 类型 | 规范 | 示例 |
|------|------|------|
| MongoDB 数据库 | `snake_case` | `fraud_detection` |
| MongoDB 集合 | `snake_case` | `fraud_alerts` |
| Kafka Topic | 简短小写 | `txns` |
| Delta Table 路径 | 无特殊字符 | `data/raw`, `data/clean` |

### 2.4 字段命名

| 层级 | 规范 | 示例 |
|------|------|------|
| Kafka JSON | `snake_case` | `transaction_id`, `user_id` |
| Spark DataFrame | `snake_case` | `kafka_partition`, `ingest_time` |
| MongoDB Document | `snake_case` | `alert_id`, `rule_type` |

**全项目字段命名统一为 snake_case，无混用情况。**

---

## 三、代码格式规范

### 3.1 工具链建议（当前未引入）

当前项目**未配置**任何 Linter 或 Formatter。建议补充：

| 工具 | 用途 | 配置文件 |
|------|------|---------|
| **Ruff** | Lint + Format (替代 Flake8 + Black + isort) | `pyproject.toml` |
| **mypy** | 类型检查 | `pyproject.toml` |

### 3.2 推荐配置

在 `finsentinel/pyproject.toml` 中添加：

```toml
[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "B", "C4", "SIM"]
ignore = ["E501"]  # 行长由 formatter 处理

[tool.ruff.format]
quote-style = "double"
indent-style = "space"

[tool.ruff.lint.isort]
known-first-party = ["config", "src"]

[tool.mypy]
python_version = "3.10"
ignore_missing_imports = true
```

### 3.3 当前风格检查清单

| 规则 | 项目状态 | 说明 |
|------|---------|------|
| 缩进: 4 空格 | ✓ | 全项目一致 |
| 行宽: ≤100 字符 | ✓ | 无明显超长行 |
| 字符串引号: 双引号 | ✗ | **混用单双引号**。如 `src/` 中双引号为主，但 YAML 解析相关处用了单引号 |
| 行尾无空格 | ✓ | 无明显行尾空格 |
| 文件尾有换行 | ~ | 部分文件无结尾空行 |
| import 分组 | ✗ | 无标准库/第三方/本地分组 |
| 逗号后空格 | ✓ | — |
| 操作符两侧空格 | ✓ | — |

**重点问题**: 字符串引号和 import 顺序混用需统一。

---

## 四、Import 规范

### 4.1 当前问题

当前项目通过 `sys.path.insert()` 进行跨包导入，**不符合 Python 包管理最佳实践**：

```python
# 反模式 — 出现在多个文件中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import get_config
```

问题：
1. 依赖 import 顺序（必须在前）
2. IDE 无法正常解析跳转
3. 测试/生产环境路径行为不一致

### 4.2 推荐方案

方案 A（最小改动）：将 `config/` 移入 `src/` 成为子包

```
src/
├── config/
│   ├── __init__.py
│   ├── settings.py
│   └── fraud_rules.yaml
├── ...
```

```python
# 标准导入
from src.config.settings import get_config
```

方案 B（更规范）：使用 `pip install -e .` 安装为可编辑包

```python
# setup.py / pyproject.toml 中声明 packages
from finsentinel.config.settings import get_config
```

### 4.3 Import 顺序规范

```python
# 1. 标准库
import os
import sys

# 2. 第三方库
from pymongo import MongoClient
from pyspark.sql import SparkSession

# 3. 本地模块
from src.schemas import transaction_schema
from src.fraud_rules import apply_all_rules
```

---

## 五、Docstring 规范

### 5.1 当前风格

项目使用**中文单行** docstring，格式为：

```python
def check_velocity(df, window_minutes=5, threshold=5):
    """R1: 速度欺诈 - 同一用户窗口内交易次数超阈值"""
```

### 5.2 规范要求

| 要求 | 说明 |
|------|------|
| 所有公共函数/类必须有 docstring | ✓ 当前已满足 |
| 语言：中文 | ✓ 与项目语言一致 |
| 格式：单行描述 + 可选的参数/返回值 | ✗ 当前无参数说明 |
| 内部/私有函数可省略 | ✓ `_build_trigger_detail` 有 docstring |

### 5.3 推荐增强格式

```python
def check_velocity(df: DataFrame, window_minutes: int = 5, threshold: int = 5) -> DataFrame:
    """R1: 速度欺诈 - 同一用户滑动窗口内交易次数超阈值。

    Args:
        df: 含 timestamp, user_id 列的交易 DataFrame。
        window_minutes: 滑动窗口大小（分钟）。
        threshold: 触发告警的最小交易次数。

    Returns:
        过滤后的告警 DataFrame，额外包含 txn_count 列。
    """
```

---

## 六、配置管理规范

### 6.1 配置分层

| 层级 | 文件 | 用途 | 修改后需重启 |
|------|------|------|-------------|
| 环境变量 | `.env` | 连接地址、路径 | 容器 / 进程 |
| YAML 配置 | `config/fraud_rules.yaml` | 业务规则阈值 | Spark 作业 |
| 代码常量 | `src/fraud_rules.py:7` | PII 盐值 | 容器重新构建 |

### 6.2 当前问题

1. **PII 盐值硬编码** (`fraud_rules.py:7`):
   ```python
   PII_SALT = "FinSentinel2024"  # 应移至环境变量
   ```
   建议：读环境变量 `PII_SALT`，无值时报错。

2. **delta_writer.py 中盐值重复定义** (`delta_writer.py:26`):
   ```python
   def process_clean_table(df, salt: str = "FinSentinel2024"):
   ```
   两个文件各自定义同一常量，违反 DRY 原则。

3. **mongo_writer.py 中阈值重复** (`mongo_writer.py:8-27`):
   `_TRIGGER_RULES` 中的阈值与 `fraud_rules.yaml` 重复。建议从 YAML 读取或从单一来源派生。

### 6.3 规范要求

| 规则 | 说明 |
|------|------|
| 单一数据源 | 同一配置只在一处定义 |
| 密钥不进 Git | `.env` 在 `.gitignore` 中 |
| 有 `.env.example` | 提供完整模板 |
| 配置有默认值 | `os.getenv("KEY", "default")` |
| 环境变量名统一前缀 | `KAFKA_`, `MONGO_`, `DELTA_`, `GEN_` 前缀规范 |

---

## 七、错误处理与日志规范

### 7.1 当前问题：依赖 print() 而非 logging

**全项目使用 `print()` 进行日志输出**，无 `logging` 模块使用：

```python
# mongo_writer.py:53
print(f"[ERROR] MongoDB write failed after {max_retries} retries: {e}")

# mongo_writer.py:56
print(f"[RETRY] MongoDB attempt {attempt + 1} failed, retrying in {wait}s...")

# spark_streaming.py:82
print("Streaming pipeline started. Waiting for data...")
```

问题：
1. 无日志级别控制（INFO / WARN / ERROR）
2. 无时间戳
3. 无法输出到文件
4. 无法按级别过滤

### 7.2 日志规范建议

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# 使用
logger.info("Streaming pipeline started")
logger.warning("MongoDB retry attempt %d", attempt + 1)
logger.error("MongoDB write failed: %s", e)
```

### 7.3 错误处理现状

| 组件 | 策略 | 评价 |
|------|------|------|
| Kafka Producer | `retries=3` + `retry_backoff_ms=1000` | ✓ Kafka 内置重试 |
| MongoDB | 手动指数退避重试 (max 3) | ✓ 良好实现 |
| Spark Streaming | `failOnDataLoss=false` | △ 牺牲一致性换取可用性 |
| transaction_generator | `try/finally` 确保 producer close | ✓ 资源安全 |
| alert_monitor | `try/finally` 确保 client close | ✓ 资源安全 |
| 全局异常 | **无全局异常捕获** | ✗ 子进程崩溃可能无日志 |

### 7.4 错误处理规范

| 规则 | 说明 |
|------|------|
| 外部 I/O 必须 try/except | Kafka、Mongo、文件读写 |
| 资源必须 finally 释放 | Producer、Client、SparkSession |
| 重试需有上限 | 最多 3-5 次 |
| 重试需指数退避 | `wait = 2 ** attempt` |
| 不吞异常 | 重试耗尽后必须 raise 或记录致命日志 |
| 使用具体异常类型 | `except (AutoReconnect, ConnectionFailure)` 而非 `except Exception` |

---

## 八、测试规范

### 8.1 测试框架与运行

```bash
pytest tests/ -v
```

### 8.2 当前测试覆盖

| 测试文件 | 用例数 | 覆盖内容 |
|---------|--------|---------|
| `test_fraud_rules.py` | 10 | R1~R4 命中/不命中场景 |
| `test_schema.py` | 7 | Schema 字段完整性、可空性、空 DataFrame 创建 |
| **合计** | **17** | |

### 8.3 测试规范要求

| 规则 | 当前状态 | 说明 |
|------|---------|------|
| 每个规则至少 1 个命中 + 1 个不命中 | ✓ | R1~R4 均有 |
| border/edge cases | ✗ | 缺少边界值测试（如恰好等于阈值） |
| Schema 完整性测试 | ✓ | `test_schema.py` 全覆盖 |
| 集成测试 | ✗ | 无端到端测试 |
| 使用 fixture 管理 SparkSession | ✓ | session 级别复用 |
| 测试函数命名: `test_<模块>_<场景>` | ✓ | — |
| 测试数据直接构造不可依赖外部 | ✓ | 全部用 `spark.createDataFrame()` |
| Spark 使用 `local[*]` | ✓ | 不依赖集群 |

### 8.4 建议补充的测试

```python
# 边界值测试
def test_check_velocity_exactly_at_threshold(spark):
    """恰好 5 笔不应命中（threshold 是 > 而非 >=）"""
    ...

# 异常场景测试
def test_check_geo_jump_single_transaction(spark):
    """单笔交易无法计算 lag，应返回空"""
    ...

# 集成测试
def test_end_to_end_pipeline(spark):
    """模拟 Kafka 消息 → 规则检测 → 告警产出"""
    ...

# 配置加载测试
def test_fraud_rules_yaml_loads():
    """验证 YAML 格式正确且所有必需字段存在"""
    ...
```

---

## 九、代码评审重点关注项

### 9.1 当前代码中的已知技术债务

| 编号 | 位置 | 问题 | 严重度 | 修复建议 |
|------|------|------|--------|---------|
| **TD-01** | `src/fraud_rules.py:7`, `src/delta_writer.py:26` | PII 盐值硬编码且重复 | **高** | 统一从环境变量 `PII_SALT` 读取 |
| **TD-02** | `src/mongo_writer.py:8-27` | 阈值与 `fraud_rules.yaml` 重复定义 | **中** | 从 YAML 读取 `_TRIGGER_RULES` 的阈值 |
| **TD-03** | 多个文件 | `sys.path.insert()` 跨包导入 hack | **中** | 重构为标准 Python 包结构 |
| **TD-04** | 全项目 | 使用 `print()` 而非 `logging` | **中** | 全局替换为 logging 模块 |
| **TD-05** | `src/mongo_writer.py:92` | `alert_id` 生成依赖 `datetime.now()`，可能与第一次创建的时间不同 | **低** | 使用 `batch_df` 中的时间戳 |
| **TD-06** | `src/alert_monitor.py:30` | 查询使用 ISO 字符串比较，时区问题可能导致遗漏 | **低** | 统一使用 UTC 时间戳比较 |
| **TD-07** | `src/spark_streaming.py:40` | `failOnDataLoss=false` — 数据丢失时静默跳过 | **中** | 生产环境建议监控 data loss 指标 |
| **TD-08** | `docker-compose.yml` | 无 Spark Worker 节点，仅 Master 运行 | **低** | 适合单机开发，生产需加 Worker |
| **TD-09** | 全项目 | 无类型注解 | **低** | 逐步添加 type hints |
| **TD-10** | `src/fraud_rules.py:132` | `df.filter(F.lit(False))` 作为空结果回退 | **低** | 可接受，但建议统一返回 `spark.createDataFrame([], alert_schema)` |

### 9.2 Review Checklist

| 检查项 | 检查方法 |
|--------|---------|
| 无硬编码凭据/密钥/地址 | `grep -r "localhost\|192.168\|password\|secret" src/` |
| 外部输入有校验 | 检查 JSON schema 解析处 |
| 资源有释放 | 检查 `close()` 调用和 `finally` 块 |
| 错误有日志 | 检查 `except` 块中是否有输出 |
| 模块 ≤ 200 行 | `wc -l src/*.py` |
| 测试覆盖新功能 | 检查 `tests/` 对应文件 |
| YAML 格式正确 | `python -c "import yaml; yaml.safe_load(open('config/fraud_rules.yaml'))"` |

---

## 十、Git 使用规范

### 10.1 仓库状态

当前仓库在 `master` 分支，**尚无任何 commit**（所有文件处于 staged 状态）。

### 10.2 首次提交建议

```bash
git commit -m "feat: init FinSentinel fraud detection platform

- Kafka + Spark Structured Streaming + Delta Lake pipeline
- 4 heuristic fraud detection rules (velocity, geo-jump, night-large, merchant-velocity)
- PII masking with SHA256 + IP truncation
- MongoDB alert storage with real-time terminal monitor
- Docker Compose full-stack deployment"
```

### 10.3 Commit Message 规范

使用 Conventional Commits 格式：

```
<type>(<scope>): <subject>

[可选 body]
```

| type | 说明 |
|------|------|
| `feat` | 新功能 |
| `fix` | Bug 修复 |
| `refactor` | 代码重构 |
| `test` | 测试相关 |
| `docs` | 文档变更 |
| `config` | 配置变更 |
| `perf` | 性能优化 |

示例：
```
feat(rules): add R5 device-fingerprint fraud rule
fix(writer): handle MongoDB connection timeout gracefully
refactor(config): extract PII_SALT to env variable
test(fraud): add edge-case tests for geo_jump threshold
docs: add ops-deployment guide
```

### 10.4 `.gitignore` 检查

当前 `.gitignore` 已覆盖：
- Python bytecode (`__pycache__/`, `*.pyc`)
- 虚拟环境 (`venv/`, `.venv/`)
- Delta Lake 数据 (`data/`)
- 环境变量 (`.env`)
- IDE 文件 (`.vscode/`, `.idea/`)
- Jupyter checkpoints

**已完备，无需修改。**

---

## 十一、后续优化路线图

### 阶段一：代码质量（优先级高）

1. **引入 Ruff** — 自动格式化 + Lint
2. **消除 `sys.path` hack** — 标准化包导入
3. **print() → logging** — 全项目替换
4. **提取 PII_SALT** 到环境变量
5. **消除阈值重复定义** — `fraud_rules.yaml` 作为单一数据源

### 阶段二：测试增强（优先级中）

6. **增加边界值测试**
7. **增加集成测试**（端到端 pipeline）
8. **添加 CI** — GitHub Actions 自动运行 pytest + lint

### 阶段三：生产就绪（优先级中）

9. **MongoDB 认证** — 容器启动时设置 root 密码
10. **健康检查接口** — 添加简单的 HTTP health endpoint
11. **指标监控** — 暴露 Prometheus metrics（alert 数量、处理延迟）
12. **Kafka SASL/SSL** — 传输加密

### 阶段四：架构演进（优先级低）

13. **ML 模型集成** — 在 R1-R4 基础上增加 ML 评分
14. **Kubernetes 部署** — 从 docker-compose 迁移到 Helm Chart
15. **告警通知渠道** — 邮件/钉钉/企业微信通知
16. **Web Dashboard** — 告警可视化管理界面

---

## 十二、检查清单（自动化校验脚本）

```bash
#!/bin/bash
# code-check.sh — 代码规范自检脚本

echo "=== 1. 文件行数检查 ==="
for f in src/*.py; do
    lines=$(wc -l < "$f")
    if [ "$lines" -gt 200 ]; then
        echo "[FAIL] $f: $lines 行 (上限 200)"
    else
        echo "[ OK ] $f: $lines 行"
    fi
done

echo ""
echo "=== 2. 硬编码检查 ==="
grep -rn "localhost\|192.168\|password\|secret" src/ config/ && echo "[WARN] 发现疑似硬编码" || echo "[ OK ] 未发现硬编码"

echo ""
echo "=== 3. print() 使用检查 ==="
count=$(grep -rc "print(" src/ | grep -v ":0$" | wc -l)
echo "[INFO] $count 个文件使用了 print() (建议替换为 logging)"

echo ""
echo "=== 4. sys.path 检查 ==="
grep -rn "sys.path" src/ && echo "[WARN] 存在 sys.path hack" || echo "[ OK ] 无 sys.path hack"

echo ""
echo "=== 5. 测试运行 ==="
pytest tests/ -v --tb=short
```

---

## 附录 A：配置文件对照表

| 配置项 | 定义位置 | 使用位置 | 是否重复 |
|--------|---------|---------|---------|
| Kafka bootstrap servers | `.env` → `settings.py` | `spark_streaming.py`, `transaction_generator.py` | ✗ |
| Mongo URI | `.env` → `settings.py` | `mongo_writer.py`, `alert_monitor.py` | ✗ |
| Delta paths | `.env` → `settings.py` | `spark_streaming.py`, `delta_writer.py` | ✗ |
| Generator config | `.env` → `settings.py` | `transaction_generator.py` | ✗ |
| R1-R4 规则阈值 | `fraud_rules.yaml` | `fraud_rules.py` | ✗ |
| **PII_SALT** | **`fraud_rules.py:7`** | `fraud_rules.py`, `delta_writer.py` | **✗ 重复定义** |
| **_TRIGGER_RULES 阈值** | **`mongo_writer.py:8`** | `mongo_writer.py` | **✗ 与 YAML 重复** |

## 附录 B：代码度量

| 指标 | 数值 |
|------|------|
| 源代码文件数 | 7 |
| 测试文件数 | 2 |
| 总代码行 | ~560 |
| 测试用例数 | 17 |
| 外部依赖数 | 7 |
| 容器服务数 | 4 |
| 最大单文件行数 | 133 (`transaction_generator.py`) |
| 技术债务项 | 10 |
