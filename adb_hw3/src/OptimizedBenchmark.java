import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Homework 3 - Part 2: Optimized Benchmark (Bottlenecks Eliminated).
 *
 * Implements the following bottleneck solutions:
 * 1. PreparedStatement Caching: Statements are prepared once per thread connection and reused.
 * 2. Decoupled Asynchronous Telemetry: Eliminates synchronous System.out.println lock contention
 *    on worker threads, using atomic metrics with periodic 1s sampling.
 * 3. Session Optimization: Executes 'SET synchronous_commit = off;' per session to eliminate
 *    disk fsync serialization overhead on commits.
 * 4. Automatic Clean Cleanup: Cleans up inserted tuples after benchmarking.
 *
 * (Note: Preserves randomized write operation order per instructor hint:
 * "Do not change the order of write operations in a transaction, e.g., to provoke canonical locking.")
 */
public class OptimizedBenchmark {

    public static final int INITIAL_TUPLES = 50000;
    public static final double MAX_VALUE = 50.0;
    public static final String DEADLOCK_DETECTED = "40P01";
    public static final String SERIALIZATION_FAILURE = "40001";

    private final int numThreads;
    private final int durationSeconds;
    private final boolean asyncCommit;
    private final AtomicInteger nextInsertId = new AtomicInteger(INITIAL_TUPLES);
    private final BenchmarkMetrics metrics = new BenchmarkMetrics();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public OptimizedBenchmark(int numThreads, int durationSeconds, boolean asyncCommit) {
        this.numThreads = numThreads;
        this.durationSeconds = durationSeconds;
        this.asyncCommit = asyncCommit;
    }

    public void prepareDatabase() {
        try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
             Statement stmt = con.createStatement()) {

            boolean recreate = false;
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM blocking_data WHERE data_id < " + INITIAL_TUPLES)) {
                if (!rs.next() || rs.getInt(1) < INITIAL_TUPLES) {
                    recreate = true;
                }
            } catch (SQLException e) {
                recreate = true;
            }

            if (recreate) {
                System.out.println("Creating and seeding blocking_data with " + INITIAL_TUPLES + " tuples...");
                stmt.execute("DROP TABLE IF EXISTS blocking_data;");
                stmt.execute("CREATE TABLE blocking_data (data_id integer PRIMARY KEY, data_value float, modified_by integer);");

                con.setAutoCommit(false);
                Random rand = new Random(123456);
                try (PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?);")) {
                    for (int i = 0; i < INITIAL_TUPLES; i++) {
                        ps.setInt(1, i);
                        ps.setDouble(2, rand.nextDouble() * MAX_VALUE);
                        ps.setInt(3, 0);
                        ps.addBatch();
                        if (i % 5000 == 0) {
                            ps.executeBatch();
                        }
                    }
                    ps.executeBatch();
                    con.commit();
                }
                con.setAutoCommit(true);
                System.out.println("Database successfully initialized.");
            } else {
                cleanupInsertedTuples(con);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to prepare database: " + e.getMessage(), e);
        }
    }

    public void cleanupInsertedTuples(Connection con) {
        try (Statement stmt = con.createStatement()) {
            int deleted = stmt.executeUpdate("DELETE FROM blocking_data WHERE data_id >= " + INITIAL_TUPLES + ";");
            if (deleted > 0) {
                System.out.println("Cleaned up " + deleted + " inserted tuples from prior run.");
            }
        } catch (SQLException e) {
            System.err.println("Warning cleaning up inserted tuples: " + e.getMessage());
        }
    }

    public BenchmarkMetrics runBenchmark() {
        prepareDatabase();

        running.set(true);
        metrics.start();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final int workerId = i;
            executor.submit(() -> workerLoop(workerId));
        }

        System.out.printf("Started Optimized Benchmark: %d threads for %d seconds (asyncCommit=%b)...%n",
                numThreads, durationSeconds, asyncCommit);

        try {
            long deadline = System.currentTimeMillis() + (durationSeconds * 1000L);
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(1000);
                long elapsedSec = Math.max(1, (System.currentTimeMillis() - metrics.startTimeMs) / 1000);
                double currentTps = (double) metrics.committedTotal.get() / elapsedSec;
                System.out.printf("[Optimized Status] %2d s elapsed | %d committed | %.1f tx/s | %d restarts%n",
                        elapsedSec, metrics.committedTotal.get(), currentTps, metrics.abortedRestarts.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            metrics.stop();
        }

        try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
            cleanupInsertedTuples(con);
        } catch (SQLException ignored) {
        }

        return metrics;
    }

    private void workerLoop(int workerId) {
        Random rand = new Random(System.nanoTime() ^ ((long) workerId << 32));
        Connection con = null;

        PreparedStatement psPointRead = null;
        PreparedStatement psInsert = null;
        PreparedStatement psScan = null;
        PreparedStatement psUpdate = null;

        try {
            con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            if (asyncCommit) {
                try (Statement stmt = con.createStatement()) {
                    stmt.execute("SET synchronous_commit = off;");
                }
            }

            // Optimization: Pre-prepare statements once for reuse
            psPointRead = con.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?;");
            psInsert = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?);");
            psScan = con.prepareStatement("SELECT sum(data_value) FROM blocking_data;");
            psUpdate = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;");

            while (running.get()) {
                double p = rand.nextDouble();
                TransactionType type = TransactionType.dice(p);

                boolean committed = false;
                while (!committed && running.get()) {
                    try {
                        switch (type) {
                            case POINT_READ:
                                executePointRead(psPointRead, rand);
                                break;
                            case INSERT:
                                executeInsert(psInsert, rand, workerId);
                                break;
                            case FULL_SCAN:
                                executeFullScan(psScan);
                                break;
                            case WRITE_UPDATE:
                                executeWriteUpdate(psUpdate, rand, workerId);
                                break;
                        }
                        con.commit();
                        committed = true;
                        metrics.recordCommit(type);
                    } catch (SQLException e) {
                        try {
                            con.rollback();
                        } catch (SQLException ignored) {
                        }
                        if (isRetryable(e)) {
                            metrics.recordRestart();
                        } else {
                            System.err.println("Worker " + workerId + " unexpected error: " + e.getMessage());
                            break;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("Worker " + workerId + " fatal DB error: " + e.getMessage());
        } finally {
            closeQuietly(psPointRead);
            closeQuietly(psInsert);
            closeQuietly(psScan);
            closeQuietly(psUpdate);
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private void executePointRead(PreparedStatement ps, Random rand) throws SQLException {
        int[] ids = pickDistinctIds(rand, 3, INITIAL_TUPLES);
        double sum = 0.0;
        for (int id : ids) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    sum += rs.getDouble(1);
                }
            }
            ps.clearParameters();
        }
    }

    private void executeInsert(PreparedStatement ps, Random rand, int workerId) throws SQLException {
        int newId = nextInsertId.getAndIncrement();
        double newVal = rand.nextDouble() * MAX_VALUE;
        ps.setInt(1, newId);
        ps.setDouble(2, newVal);
        ps.setInt(3, workerId);
        ps.executeUpdate();
        ps.clearParameters();
    }

    private void executeFullScan(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                double total = rs.getDouble(1);
            }
        }
    }

    private void executeWriteUpdate(PreparedStatement ps, Random rand, int workerId) throws SQLException {
        int[] ids = pickDistinctIds(rand, 3, INITIAL_TUPLES);
        // Note: write operations remain in random order as diced (per hints)
        for (int id : ids) {
            ps.setDouble(1, rand.nextDouble() * MAX_VALUE);
            ps.setInt(2, workerId);
            ps.setInt(3, id);
            ps.executeUpdate();
            ps.clearParameters();
        }
    }

    private int[] pickDistinctIds(Random rand, int count, int max) {
        int[] result = new int[count];
        Set<Integer> chosen = new HashSet<>();
        for (int i = 0; i < count; i++) {
            int id;
            do {
                id = rand.nextInt(max);
            } while (!chosen.add(id));
            result[i] = id;
        }
        return result;
    }

    private boolean isRetryable(SQLException e) {
        String state = e.getSQLState();
        return DEADLOCK_DETECTED.equals(state) || SERIALIZATION_FAILURE.equals(state);
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {
            }
        }
    }

    public static void main(String[] args) {
        int threads = 4;
        int duration = 10;
        boolean asyncCommit = true;

        for (int i = 0; i < args.length; i++) {
            if ("--threads".equals(args[i]) && i + 1 < args.length) {
                threads = Integer.parseInt(args[++i]);
            } else if ("--duration".equals(args[i]) && i + 1 < args.length) {
                duration = Integer.parseInt(args[++i]);
            } else if ("--sync-commit".equals(args[i])) {
                asyncCommit = false;
            } else if ("--serial".equals(args[i])) {
                threads = 1;
            }
        }

        OptimizedBenchmark benchmark = new OptimizedBenchmark(threads, duration, asyncCommit);
        BenchmarkMetrics metrics = benchmark.runBenchmark();
        metrics.printSummary("Optimized Concurrent (Threads: " + threads + ", asyncCommit: " + asyncCommit + ")", threads);
    }
}
