# Advanced Database Systems (Winter 2026) - Homework 2

Task 2 of Winter Semester 2026/27 Advanced Database Systems proseminar (`adb_2026_task_2`).

---

## 📁 Repository Structure

```text
adb_hw2/
├── lib/
│   └── postgresql-42.2.18.jar   # PostgreSQL JDBC Driver
├── src/
│   ├── ConnectionFactory.java   # PostgreSQL JDBC connection setup
│   ├── Blocking.java            # Task 2: Reproducing deadlocks & row-level locks
│   ├── RestartBlocking.java     # Task 3: Automatic restart on transaction aborts
│   └── NoLiveLocks.java         # Task 4: Starvation & livelock avoidance
├── schema.sql                   # Database table definitions
├── LICENSE                      # Project license
└── README.md                    # Homework 2 documentation
```

---

## 📝 Source Code & Tasks Overview

- **`src/Blocking.java` (Task 2)**:
  Demonstrates concurrency control issues, row-level locks, and reproduction of deadlock scenarios under PostgreSQL.

- **`src/RestartBlocking.java` (Task 3)**:
  Implements automatic restart and backoff strategies when transactions are aborted due to serialization failures or deadlocks.

- **`src/NoLiveLocks.java` (Task 4)**:
  Implements livelock avoidance mechanisms ensuring starvation-free transaction completion.

- **`src/ConnectionFactory.java`**:
  Manages JDBC connections to the PostgreSQL database with fallback to environment variables (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`).

- **`schema.sql`**:
  SQL schema required for setting up database tables.

---

## 🚀 Compilation & Execution Guide

### 1. Compile Source Files
Compile all Java source files into the `bin/` directory:
```bash
javac -cp "lib/postgresql-42.2.18.jar" -d bin src/*.java
```

### 2. Run Tasks

#### Run Task 2 (Blocking & Deadlocks)
```bash
java -cp "bin:lib/postgresql-42.2.18.jar" Blocking
```

#### Run Task 3 (Restart on Transaction Abort)
```bash
java -cp "bin:lib/postgresql-42.2.18.jar" RestartBlocking
```

#### Run Task 4 (Livelock Avoidance)
```bash
java -cp "bin:lib/postgresql-42.2.18.jar" NoLiveLocks
```
