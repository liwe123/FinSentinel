# 大数据学期项目 — FinSentinel 实时欺诈检测平台

Direction A: Real-time Fraud Detection & Risk Monitoring Platform

## 项目结构

```
.
├── finsentinel/                 # 核心项目 — Kafka + Spark + Delta Lake 实时欺诈检测
│   ├── src/                     # 源代码
│   ├── config/                  # 配置文件
│   ├── tests/                   # 测试用例
│   ├── notebooks/               # Jupyter notebooks
│   └── docker-compose.yml       # 容器编排
├── docs/                        # 项目文档
│   ├── project-plan.md          # 学期项目计划
│   ├── project-plan.pdf         # 学期项目计划 (PDF)
│   └── project-plan-detail.md   # 详细计划书
├── tools/                       # 辅助工具
│   └── pdf2md/                  # PDF 转 Markdown 工具
└── .gitignore
```

## 快速开始

详见 `finsentinel/README.md`

## 技术栈

- **消息队列**: Apache Kafka (Confluent 7.5.0)
- **流处理**: Apache Spark Structured Streaming 3.5.0
- **存储**: Delta Lake 3.0.0 + MongoDB 7.0
- **容器化**: Docker Compose
