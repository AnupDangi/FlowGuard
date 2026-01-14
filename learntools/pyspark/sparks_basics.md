# Apache Spark Fundamentals

![History](images/history.png)

![Architecture diagram](images/architecture.png)

![Working diagram](images/working.png)

---

## Why Spark Exists

**Pandas** = single-machine, in-memory analysis. Works great until:

- CSV is 40+ GB
- Logs arrive continuously
- Joins span billions of rows
- You need fault tolerance

**Spark** = distributed execution engine for cluster-scale data.

---

## Core Differences

| Problem             | Pandas                    | Spark                           |
| ------------------- | ------------------------- | ------------------------------- |
| **Scale**           | MB → few GB               | GB → PB                         |
| **Execution**       | Single machine, in-memory | Distributed cluster             |
| **Fault tolerance** | ❌ (crash = restart)      | ✅ (recomputes lost partitions) |
| **Streaming**       | ❌                        | ✅ (Structured Streaming)       |
| **Parallelism**     | Cores on one box          | Tasks across cluster            |

---

## Spark vs PySpark

- **Spark**: Engine written in Scala, runs on JVM
- **PySpark**: Python API to control Spark

Python is the steering wheel. Spark is the engine.

---

## When to Use What

**Pandas**

- Quick exploration, small tables
- Debugging transformations
- Visualization

**Spark**

- Data exceeds RAM
- Production ETL pipelines
- Streaming (Kafka → Spark → Storage)
- Fault-tolerant processing

**Rule**: Use Pandas for thinking, Spark for moving and scaling data.

---

## How Spark Works (Core Concept)

**Lazy Evaluation**

- Operations build a DAG (Directed Acyclic Graph)
- Nothing runs until an _action_ (`count`, `collect`, `write`)
- Enables query optimization + fault recovery

**Key idea**: "Bring code to data, not data to code."

Spark partitions data → spreads across machines → executes in parallel.

Spark isn't magical—it respects physics: memory, network, time, failure.
