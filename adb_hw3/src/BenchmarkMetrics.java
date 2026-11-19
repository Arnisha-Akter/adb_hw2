
// import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks benchmark execution statistics across worker threads.
 */
public class BenchmarkMetrics {
    public final AtomicLong committedTotal = new AtomicLong(0);
    public final AtomicLong committedPointReads = new AtomicLong(0);
    public final AtomicLong committedInserts = new AtomicLong(0);
    public final AtomicLong committedFullScans = new AtomicLong(0);
    public final AtomicLong committedUpdates = new AtomicLong(0);
    public final AtomicLong abortedRestarts = new AtomicLong(0);

    public long startTimeMs;
    public long endTimeMs;

    public void start() {
        startTimeMs = System.currentTimeMillis();
    }

    public void stop() {
        endTimeMs = System.currentTimeMillis();
    }

    public double getElapsedSeconds() {
        long end = (endTimeMs > 0) ? endTimeMs : System.currentTimeMillis();
        return Math.max(0.001, (end - startTimeMs) / 1000.0);
    }

    public double getThroughput() {
        return committedTotal.get() / getElapsedSeconds();
    }

    public void recordCommit(TransactionType type) {
        committedTotal.incrementAndGet();
        switch (type) {
            case POINT_READ:
                committedPointReads.incrementAndGet();
                break;
            case INSERT:
                committedInserts.incrementAndGet();
                break;
            case FULL_SCAN:
                committedFullScans.incrementAndGet();
                break;
            case WRITE_UPDATE:
                committedUpdates.incrementAndGet();
                break;
        }
    }

    public void recordRestart() {
        abortedRestarts.incrementAndGet();
    }

    public void printSummary(String label, int threads) {
        double elapsed = getElapsedSeconds();
        double throughput = getThroughput();

        System.out.println("===============================================================");
        System.out.println(" BENCHMARK RESULT: " + label);
        System.out.println("===============================================================");
        System.out.printf(" Threads:               %d%n", threads);
        System.out.printf(" Elapsed Time:          %.2f s%n", elapsed);
        System.out.printf(" Total Committed Tx:    %d%n", committedTotal.get());
        System.out.printf(" Throughput:            %.2f tx/sec%n", throughput);
        System.out.printf(" Aborted Restarts:      %d%n", abortedRestarts.get());
        System.out.println("---------------------------------------------------------------");
        System.out.printf(" - Point Reads (60%%):   %d (%.1f%%)%n",
                committedPointReads.get(),
                committedTotal.get() == 0 ? 0 : (100.0 * committedPointReads.get() / committedTotal.get()));
        System.out.printf(" - Inserts (10%%):       %d (%.1f%%)%n",
                committedInserts.get(),
                committedTotal.get() == 0 ? 0 : (100.0 * committedInserts.get() / committedTotal.get()));
        System.out.printf(" - Full Scans (10%%):     %d (%.1f%%)%n",
                committedFullScans.get(),
                committedTotal.get() == 0 ? 0 : (100.0 * committedFullScans.get() / committedTotal.get()));
        System.out.printf(" - Write Updates (20%%):  %d (%.1f%%)%n",
                committedUpdates.get(),
                committedTotal.get() == 0 ? 0 : (100.0 * committedUpdates.get() / committedTotal.get()));
        System.out.println("===============================================================");
    }
}
