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
 * Homework 3 - Part 1: Extended Benchmark (Baseline).
 *
 * Implements the realistic workload distribution:
 * - p < 0.6       : Multi-point read of 3 random tuples (60%)
 * - 0.6 <= p < 0.7: Single tuple insert (10%)
 * - 0.7 <= p < 0.8: Full table read-only scan (10%)
 * - p >= 0.8      : Write query with 3 single-tuple updates (20%)
 *
 * Maintains a continuous stream of transactions in equilibrium.
 * Aborted transactions (e.g. deadlocks 40P01) are rolled back and restarted until commit.
 */
public class ExtendedBlockingBenchmark {

    public static final int INITIAL_TUPLES = 50000;
    public static final double MAX_VALUE = 50.0;
    public static final String DEADLOCK_DETECTED = "40P01";
    public static final String SERIALIZATION_FAILURE = "40001";

    private final int numThreads;
    private final int durationSeconds;
    private final boolean verboseConsole;
    private final AtomicInteger nextInsertId = new AtomicInteger(INITIAL_TUPLES);
    private final BenchmarkMetrics metrics = new BenchmarkMetrics();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ExtendedBlockingBenchmark(int numThreads, int durationSeconds, boolean verboseConsole) {
        this.numThreads = numThreads;
        this.durationSeconds = durationSeconds;
        this.verboseConsole = verboseConsole;
    }

    /**
     * Resets or prepares the table blocking_data with INITIAL_TUPLES rows.
     */
    public void prepareDatabase() {
        try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
             Statement stmt = con.createStatement()) {

            // Check if table exists and has initial tuples
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
                // Remove leftover insert tuples from prior benchmark runs
                cleanupInsertedTuples(con);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to prepare database: " + e.getMessage(), e);
        }
    }

    /**
     * Cleans up tuples added during the benchmark (data_id >= INITIAL_TUPLES).
     */
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

        System.out.printf("Started Baseline Benchmark: %d threads for %d seconds (verbose=%b)...%n",
                numThreads, durationSeconds, verboseConsole);

        // Run for specified duration
        try {
            long deadline = System.currentTimeMillis() + (durationSeconds * 1000L);
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(1000);
                long elapsedSec = Math.max(1, (System.currentTimeMillis() - metrics.startTimeMs) / 1000);
                double currentTps = (double) metrics.committedTotal.get() / elapsedSec;
                System.out.printf("[Status] %2d s elapsed | %d committed | %.1f tx/s | %d restarts%n",
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

        // Final cleanup of inserted tuples
        try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
            cleanupInsertedTuples(con);
        } catch (SQLException ignored) {
        }

        return metrics;
    }

    private void workerLoop(int workerId) {
        Random rand = new Random(System.nanoTime() ^ ((long) workerId << 32));
        Connection con = null;

        try {
            con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            while (running.get()) {
                double p = rand.nextDouble();
                TransactionType type = TransactionType.dice(p);

                boolean committed = false;
                while (!committed && running.get()) {
                    try {
                        switch (type) {
                            case POINT_READ:
                                executePointRead(con, rand, workerId);
                                break;
                            case INSERT:
                                executeInsert(con, rand, workerId);
                                break;
                            case FULL_SCAN:
                                executeFullScan(con, workerId);
                                break;
                            case WRITE_UPDATE:
                                executeWriteUpdate(con, rand, workerId);
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
                            // Restart transaction until commit
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
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private void executePointRead(Connection con, Random rand, int workerId) throws SQLException {
        int[] ids = pickDistinctIds(rand, 3, INITIAL_TUPLES);
        double sum = 0.0;
        try (PreparedStatement ps = con.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?;")) {
            for (int id : ids) {
                ps.setInt(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        sum += rs.getDouble(1);
                    }
                }
            }
        }
        if (verboseConsole) {
            System.out.printf("[Worker-%d] Point read sum of ids [%d, %d, %d] = %.2f%n",
                    workerId, ids[0], ids[1], ids[2], sum);
        }
    }

    private void executeInsert(Connection con, Random rand, int workerId) throws SQLException {
        int newId = nextInsertId.getAndIncrement();
        double newVal = rand.nextDouble() * MAX_VALUE;
        try (PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?);")) {
            ps.setInt(1, newId);
            ps.setDouble(2, newVal);
            ps.setInt(3, workerId);
            ps.executeUpdate();
        }
        if (verboseConsole) {
            System.out.printf("[Worker-%d] Inserted tuple data_id=%d, value=%.2f%n", workerId, newId, newVal);
        }
    }

    private void executeFullScan(Connection con, int workerId) throws SQLException {
        double totalSum = 0.0;
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT sum(data_value) FROM blocking_data;")) {
            if (rs.next()) {
                totalSum = rs.getDouble(1);
            }
        }
        if (verboseConsole) {
            System.out.printf("[Worker-%d] Full table scan sum = %.2f%n", workerId, totalSum);
        }
    }

    private void executeWriteUpdate(Connection con, Random rand, int workerId) throws SQLException {
        int[] ids = pickDistinctIds(rand, 3, INITIAL_TUPLES);
        // Note: per assignment hints: "Do not change the order of write operations in a transaction,
        // e.g., to provoke canonical locking." Keep order as diced!
        try (PreparedStatement ps = con.prepareStatement(
                "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;")) {
            for (int id : ids) {
                ps.setDouble(1, rand.nextDouble() * MAX_VALUE);
                ps.setInt(2, workerId);
                ps.setInt(3, id);
                ps.executeUpdate();
            }
        }
        if (verboseConsole) {
            System.out.printf("[Worker-%d] Updated 3 tuples: [%d, %d, %d]%n", workerId, ids[0], ids[1], ids[2]);
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

    public static void main(String[] args) {
        int threads = 4;
        int duration = 10;
        boolean verbose = false;

        for (int i = 0; i < args.length; i++) {
            if ("--threads".equals(args[i]) && i + 1 < args.length) {
                threads = Integer.parseInt(args[++i]);
            } else if ("--duration".equals(args[i]) && i + 1 < args.length) {
                duration = Integer.parseInt(args[++i]);
            } else if ("--verbose".equals(args[i])) {
                verbose = true;
            } else if ("--serial".equals(args[i])) {
                threads = 1;
            }
        }

        ExtendedBlockingBenchmark benchmark = new ExtendedBlockingBenchmark(threads, duration, verbose);
        BenchmarkMetrics metrics = benchmark.runBenchmark();
        metrics.printSummary("Baseline Concurrent (Threads: " + threads + ")", threads);
    }
}
