# 🚀 Fraud Detection Streaming Platform

Real-time fraud detection system built with a modern data stack: **Kafka + Spark Structured Streaming + PostgreSQL + Streamlit**.

<img width="958" height="475" alt="image" src="https://github.com/user-attachments/assets/1ab40c47-fbbd-4d48-8aae-b1e6ca17ca3c" />


---

## 📸 Overview

This project simulates a **real-world banking fraud detection system**, capable of ingesting, processing, and detecting anomalies in streaming transactions.

---

## 🏗️ Architecture

```mermaid
flowchart LR

    A[Transaction Generator] --> B[Kafka]

    B --> C[Spark Streaming]

    C --> D[PostgreSQL]

    C --> E[Fraud Detection Engine]

    E --> F[Alerts]

    E --> G[Monitoring]

    D --> H[Streamlit Dashboard]

    F --> H
    G --> H
```
git clone https://github.com/your-repo/fraud-detection-platform.git
cd fraud-detection-platform
