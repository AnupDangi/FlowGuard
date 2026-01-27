# Apache Airflow: Workflow Orchestration

Airflow lets you define, schedule, and monitor data pipelines as code.

**Official Documentation**: https://airflow.apache.org/docs/

---

## Core Concepts

### 1. DAG (Directed Acyclic Graph)

Workflow blueprint defining task dependencies.

```python
with DAG('my_pipeline', schedule='@daily') as dag:
    fetch_data >> clean_data >> save_data
```

- **Directed**: Tasks have order (A → B)
- **Acyclic**: No loops allowed
- **Graph**: Visual representation of workflow

---

### 2. Tasks

Single unit of work in a DAG.

```python
fetch_user = PythonOperator(
    task_id='fetch_user',
    python_callable=fetch_user_function
)
```

**Task Dependencies**:

```
Linear:    task_a >> task_b >> task_c
Parallel:  task_a >> [task_b, task_c] >> task_d
```

---

### 3. Operators

Template for task execution.

| Operator              | Purpose             |
| --------------------- | ------------------- |
| `PythonOperator`      | Run Python function |
| `BashOperator`        | Run shell command   |
| `SparkSubmitOperator` | Submit Spark job    |

---

### 4. Scheduler

Decides when DAGs run based on:

- `schedule_interval` (cron or preset)
- Task dependencies
- Retry logic

```python
@dag(
    schedule="*/30 * * * *",  # Every 30 minutes
    catchup=False
)
```

---

### 5. Ingestion Pattern

Example: API → Bronze storage with partitioning

```python
@task
def fetch_users():
    response = requests.get("https://api.example.com/users")
    return response.json()

@task
def write_to_bronze(data, **context):
    logical_date = context["data_interval_start"].date()
    path = f"/data/bronze/users/dt={logical_date}/data.json"
    # Write data to path
```

**Key Concepts**:

- `data_interval_start`: Logical execution time
- Partitioning by date: `dt=2026-01-27`
- Idempotent writes: Same run_id overwrites

---

### 6. Backfill

Run DAG for historical dates.

```bash
airflow dags backfill \
  --start-date 2026-01-01 \
  --end-date 2026-01-15 \
  my_pipeline
```

**Catchup Parameter**:

```python
catchup=True   # Auto-backfill missed schedules
catchup=False  # Only run from now onwards
```

---

## Example: Bronze Ingestion DAG

```python
@dag(
    dag_id="bronze_users_ingestion",
    start_date=datetime(2026, 1, 21),
    schedule="* * * * *",  # Every minute
    catchup=True
)
def bronze_ingestion():

    @task
    def fetch_users():
        return requests.get("https://api.example.com").json()

    @task
    def write_to_bronze(data, **context):
        date = context["data_interval_start"].date()
        path = f"/bronze/users/dt={date}/users.json"
        # Write data
        return {"path": path}

    write_to_bronze(fetch_users())

bronze_ingestion()
```

---

## Airflow in Your Stack

```
Kafka → Airflow schedules Spark jobs → Data Lake
```

Airflow orchestrates when batches run, handles retries, and monitors pipeline health.
