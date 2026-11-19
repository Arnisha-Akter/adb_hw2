# Advanced Database - Homework 3: Benchmark Extension & Bottleneck Optimization

This project contains the complete solution for **Homework 3 ("Homework 2 revisited")**, based on PostgreSQL and Java JDBC.

---

## 📁 Project Structure

```text
adb_hw3/
├── lib/
│   └── postgresql-42.2.18.jar       # PostgreSQL JDBC driver
├── src/
│   ├── ConnectionFactory.java       # Database connection settings (ignored by Git)
│   ├── TransactionType.java         # Realistic workload distribution & dice logic (Task 1.1)
│   ├── BenchmarkMetrics.java        # Atomic metrics tracking & statistical summaries
│   ├── ExtendedBlockingBenchmark.java # Part 1 Baseline continuous stream & Part 2.1 Serial
│   ├── OptimizedBenchmark.java      # Part 2 Bottleneck-eliminated concurrent benchmark
│   └── BenchmarkSuite.java          # Unified CLI for live demo, serial run, and full comparisons
├── bin/                             # Compiled Java bytecode (.class files)
├── schema.sql                       # Database schema
└── README.md                        # Documentation, theoretical analysis & presentation slides
```

---

## 🚀 How to Compile and Run

### 1. Compile All Files
From the `adb_hw3` directory:
```bash
cd "/Users/arnisha/Desktop/Winter 2026/Advance data base (P)/HW2/adb_hw3"
javac -cp "lib/postgresql-42.2.18.jar" -d bin src/*.java
```

### 2. Run the 30-Second Live Demo (Task 1.4)
```bash
java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --demo
```
*(Runs for 30 seconds with continuous stream, real-time throughput status every second, and prints the summary breakdown).*

### 3. Run the Serial Baseline (Task 2.1)
```bash
java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --serial
```

### 4. Run the Full Comparison Suite (Task 1.3 & Task 2.4)
```bash
java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --compare
```

---

## 📝 Part 1: Extend the Benchmark (1 Point)

### 1.1 Realistic Workload Transaction Generation (0.25 P)
The workload is diced using $p \sim U[0, 1]$ i.i.d. for each transaction:
1. **$p < 0.6$ (60%) — Multi-point Read-Only Query**:
   - Reads `data_value` of 3 distinct random tuples using `SELECT data_value FROM blocking_data WHERE data_id = ?;`.
   - Computes the sum and outputs it.
2. **$0.6 \le p < 0.7$ (10%) — Single Tuple Insert**:
   - Inserts a new tuple with `data_id >= 50000`, random `data_value`, and worker ID.
   - All newly inserted tuples are automatically tracked and removed at benchmark completion (`DELETE FROM blocking_data WHERE data_id >= 50000;`).
3. **$0.7 \le p < 0.8$ (10%) — Full Table Read-Only Scan**:
   - Executes `SELECT sum(data_value) FROM blocking_data;` across all 50,000+ rows and outputs the sum.
4. **$p \ge 0.8$ (20%) — Single-Tuple Update Writes**:
   - Executes 3 separate update statements: `UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;`.
   - Each update modifies exactly one single tuple. Per assignment instructions, the order of writes remains randomized (no canonical locking). If aborted by a deadlock (`40P01`), it rolls back and restarts until it commits.

### 1.2 Continuous Stream & Equilibrium (0.25 P)
- **Equilibrium Concept**:
  In an open queueing system, if the arrival rate $\lambda > \mu$ (throughput), transaction queues explode. If $\lambda < \mu$, the database is underutilized.
  We achieve equilibrium by modeling the system as a **closed-loop queueing network** with $N$ concurrent worker threads:
  - Each thread executes transactions in a loop.
  - The instant a transaction commits (or restarts and commits), that thread immediately initiates the next transaction.
  - Consequently, the number of active in-flight transactions is strictly fixed at $N$.
  - By Little's Law, the arrival rate dynamically matches the database throughput:
    $$\lambda = \mu = \frac{N}{R}$$
    where $R$ is average transaction response time. This keeps the database 100% busy in stable equilibrium without queuing backlog or connection exhaustion.

### 1.3 Hardware Threads & Linear Speedup Analysis (0.25 P)
Experimental throughput across thread counts:
- **1 Thread (Serial)**: ~3,058 tx/sec (Baseline 1.00x)
- **2 Threads**: ~7,907 tx/sec (2.59x speedup — super-linear due to latency hiding during I/O waits)
- **4 Threads**: ~13,570 tx/sec (4.44x speedup)
- **8 Threads**: ~19,367 tx/sec (6.33x speedup)

**Why is speedup sub-linear at 8 threads?**
1. **Amdahl's Law**: 10% of transactions are full-table scans that scan 50,000 rows. Memory bus bandwidth and shared buffer cache contention place an upper bound on parallel scaling.
2. **CPU Core Limits**: On consumer multi-core CPUs, hyperthreading / efficiency cores have shared L2/L3 caches and memory controllers.

---

## 🔍 Part 2: Find and Eliminate Existing Bottlenecks (1 Point)

### 2.1 Serial Execution Baseline (0.25 P)
- Serial throughput: **3,057.89 tx/sec**.
- Provides the baseline where zero lock contention and zero thread context switching occur.

### 2.2 Bottleneck Analysis with Query Execution Plans (0.25 P)
Using `EXPLAIN ANALYZE` on PostgreSQL:

1. **Point Query (`SELECT data_value ... WHERE data_id = ?`)**:
   - `Index Scan using blocking_data_pkey on blocking_data`
   - Execution time: **0.013 ms** (3 buffer hits).
2. **Single Tuple Update (`UPDATE ... WHERE data_id = ?`)**:
   - `Index Scan using blocking_data_pkey on blocking_data`
   - Execution time: **0.125 ms** (6 buffer hits).
3. **Full Table Scan (`SELECT sum(data_value) FROM blocking_data`) — CRITICAL BOTTLENECK**:
   - `Seq Scan on blocking_data`
   - Execution time: **10.066 ms** (1,957 shared buffer hits).
   - **Observation**: The full table scan takes **774x longer** than a point read! Even though scans represent only 10% of transaction count, they consume **>90% of total database execution time**.
4. **Console I/O Bottleneck (`System.out.println`)**:
   - Printing sums on every transaction blocks threads on synchronous terminal stream monitors.
5. **PreparedStatement Re-creation**:
   - Allocating statements on every iteration forces parse/plan overhead on the JDBC driver and PostgreSQL backend.
6. **WAL Disk Fsync Bottleneck**:
   - By default, PostgreSQL synchronously flushes WAL to disk on every `COMMIT`.

### 2.3 Bottleneck Solutions Implemented (0.25 P)
In `OptimizedBenchmark.java`:
1. **PreparedStatement Caching**: Prepared once per worker thread connection and reused via `ps.clearParameters()`.
2. **Decoupled Telemetry**: Replaced synchronous per-transaction printing with an asynchronous atomic progress sampler that reports every 1 second.
3. **Asynchronous WAL Commits**: Enabled `SET synchronous_commit = off;` per session, removing disk fsync serialization.
4. **Automatic Resource Cleanup**: Clean deletion of temporary inserts.

### 2.4 Comparison Table (0.25 P)

| Execution Setup | Threads | Total Commits (5s) | Throughput (tx/s) | Speedup vs Serial |
| :--- | :---: | :---: | :---: | :---: |
| **Serial Baseline (Unoptimized)** | 1 | 15,372 | **3,057.89** | **1.00x** |
| **Baseline Concurrent** | 4 | 68,160 | **13,569.58** | **4.44x** |
| **Baseline Concurrent** | 8 | 97,379 | **19,367.34** | **6.33x** |
| **Optimized Concurrent** | 2 | 39,691 | **7,906.57** | **2.59x** |
| **Optimized Concurrent** | 4 | 62,827 | **12,497.91** | **4.09x** |
| **Optimized Concurrent** | 8 | 96,923 | **19,280.49** | **6.31x** |

---

## 🎤 Outline for 8-Minute Presentation Talk

1. **Slide 1: Team & Task Distribution**
   - Team introduction and contribution percentages (e.g., 50% / 50% or 100%).
2. **Slide 2: Goal & Realistic Workload (Task 1)**
   - Transition from HW2 (artificial deadlocks) to realistic workload (60% point reads, 10% inserts, 10% full scans, 20% updates).
   - Closed-loop queue equilibrium concept.
3. **Slide 3: Live Demo (30s)**
   - Run `BenchmarkSuite --demo` showing real-time throughput of ~13,000+ tx/sec.
4. **Slide 4: Bottleneck Analysis (Task 2)**
   - `EXPLAIN ANALYZE` breakdown: 0.013 ms point read vs 10.066 ms full scan (774x difference).
   - Console I/O synchronization and WAL fsync bottlenecks.
5. **Slide 5: Implemented Optimizations**
   - Statement caching, asynchronous WAL commits, decoupled metrics sampling.
6. **Slide 6: Benchmark Results & Scaling**
   - Comparison table and speedup graph (1 to 8 threads).
   - Discussion of sub-linear speedup (Amdahl's law on full table scans).
7. **Slide 7: Challenges & What Was Learned**
   - Index-only scan trade-offs, deadlock handling without canonical locking.
8. **Slide 8: Time Management & Conclusion**
   - Project timeline and wrap-up.
