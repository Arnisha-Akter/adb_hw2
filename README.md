# Advanced Database Systems (Winter 2026) - ADB-HW

This repository contains the completed assignments for Advanced Database Systems.

---

## 📁 Repository Structure

```text
ADB-HW/
├── adb_hw2/                         # Homework 2
│   ├── lib/postgresql-42.2.18.jar   # PostgreSQL JDBC Driver
│   ├── src/
│   │   ├── Blocking.java            # Task 2: Reproducing deadlocks & row-level locks
│   │   ├── RestartBlocking.java     # Task 3: Automatic restart on transaction aborts
│   │   └── NoLiveLocks.java         # Task 4: Starvation & livelock avoidance
│   ├── schema.sql                   # Database table definitions
│   └── README.md                    # Homework 2 documentation
│
└── adb_hw3/                         # Homework 3
    ├── lib/postgresql-42.2.18.jar   # PostgreSQL JDBC Driver
    ├── src/
    │   ├── ConnectionFactory.java   # Dynamic credential loader from .env
    │   ├── TransactionType.java     # Realistic workload distribution generator (60/10/10/20)
    │   ├── BenchmarkMetrics.java    # Atomic metrics tracking & statistical summaries
    │   ├── ExtendedBlockingBenchmark.java # Task 1 Baseline continuous stream & Task 2.1 Serial
    │   ├── OptimizedBenchmark.java  # Task 2.3 Bottleneck-eliminated benchmark
    │   └── BenchmarkSuite.java      # Unified CLI for live demo, serial & comparison
    ├── schema.sql                   # Database table definitions
    ├── .env.example                 # Credentials template
    └── README.md                    # Complete benchmark results, query plans & slides outline
```

---

## 🚀 Quick Execution Guide

### Homework 2
```bash
cd adb_hw2
javac -cp "lib/postgresql-42.2.18.jar" -d bin src/*.java
java -cp "bin:lib/postgresql-42.2.18.jar" RestartBlocking
java -cp "bin:lib/postgresql-42.2.18.jar" NoLiveLocks
```

### Homework 3
```bash
cd adb_hw3
javac -cp "lib/postgresql-42.2.18.jar" -d bin src/*.java

# Run 30-Second Live Demo (Task 1.4)
java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --demo

# Run Serial Baseline (Task 2.1)
java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --serial

# Run Full Comparative Suite (Task 1.3 & 2.4)
java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --compare
```
