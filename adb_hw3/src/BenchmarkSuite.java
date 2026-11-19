/**
 * Unified CLI Runner for Homework 3:
 *
 * Usage:
 *   java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --demo
 *       Runs the required 30-second live demo showing the continuous stream
 *       and real-time throughput metrics.
 *
 *   java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --serial
 *       Runs the serial execution baseline (1 thread).
 *
 *   java -cp "bin:lib/postgresql-42.2.18.jar" BenchmarkSuite --compare
 *       Runs the complete benchmark suite (Serial vs Baseline Concurrent vs Optimized Concurrent)
 *       and generates the final comparison table for the 8-minute presentation slides.
 */
public class BenchmarkSuite {

    public static void main(String[] args) {
        String mode = "--demo";
        int duration = 30;

        if (args.length > 0) {
            mode = args[0];
        }
        for (int i = 0; i < args.length; i++) {
            if ("--duration".equals(args[i]) && i + 1 < args.length) {
                duration = Integer.parseInt(args[++i]);
            }
        }

        switch (mode) {
            case "--demo":
                runLiveDemo(duration);
                break;
            case "--serial":
                runSerial(duration);
                break;
            case "--compare":
                runFullComparison();
                break;
            default:
                printUsage();
                break;
        }
    }

    private static void printUsage() {
        System.out.println("ADB Homework 3 - Benchmark Suite");
        System.out.println("Usage:");
        System.out.println("  java -cp \"bin:lib/*\" BenchmarkSuite --demo [--duration <sec>]");
        System.out.println("  java -cp \"bin:lib/*\" BenchmarkSuite --serial [--duration <sec>]");
        System.out.println("  java -cp \"bin:lib/*\" BenchmarkSuite --compare");
    }

    private static void runLiveDemo(int duration) {
        System.out.println("=================================================================");
        System.out.printf(" STARTING HOMEWORK 3 LIVE DEMO (%d SECONDS)%n", duration);
        System.out.println(" Realistic Workload: 60% Read-Point | 10% Insert | 10% Scan | 20% Update");
        System.out.println(" Closed-Loop Equilibrium: 4 Worker Threads continuous stream");
        System.out.println("=================================================================");

        OptimizedBenchmark demo = new OptimizedBenchmark(4, duration, true);
        BenchmarkMetrics metrics = demo.runBenchmark();
        metrics.printSummary("30-Second Live Demo (Optimized 4 Threads)", 4);
    }

    private static void runSerial(int duration) {
        System.out.println("=================================================================");
        System.out.printf(" RUNNING SERIAL EXECUTION BASELINE (1 THREAD, %d SECONDS)%n", duration);
        System.out.println("=================================================================");

        ExtendedBlockingBenchmark serial = new ExtendedBlockingBenchmark(1, duration, false);
        BenchmarkMetrics metrics = serial.runBenchmark();
        metrics.printSummary("Serial Execution Baseline (1 Thread)", 1);
    }

    private static void runFullComparison() {
        int duration = 5; // 5 seconds per trial for rapid, accurate multi-run comparison
        System.out.println("=================================================================");
        System.out.println(" RUNNING FULL HW3 COMPARATIVE BENCHMARK SUITE");
        System.out.println("=================================================================");

        // 1. Serial Baseline
        System.out.println("\n[1/6] Running Serial Execution (1 Thread)...");
        ExtendedBlockingBenchmark serial = new ExtendedBlockingBenchmark(1, duration, false);
        BenchmarkMetrics mSerial = serial.runBenchmark();

        // 2. Baseline Concurrent (4 threads)
        System.out.println("\n[2/6] Running Baseline Concurrent (4 Threads)...");
        ExtendedBlockingBenchmark base4 = new ExtendedBlockingBenchmark(4, duration, false);
        BenchmarkMetrics mBase4 = base4.runBenchmark();

        // 3. Baseline Concurrent (8 threads)
        System.out.println("\n[3/6] Running Baseline Concurrent (8 Threads)...");
        ExtendedBlockingBenchmark base8 = new ExtendedBlockingBenchmark(8, duration, false);
        BenchmarkMetrics mBase8 = base8.runBenchmark();

        // 4. Optimized Concurrent (2 threads)
        System.out.println("\n[4/6] Running Optimized Concurrent (2 Threads)...");
        OptimizedBenchmark opt2 = new OptimizedBenchmark(2, duration, true);
        BenchmarkMetrics mOpt2 = opt2.runBenchmark();

        // 5. Optimized Concurrent (4 threads)
        System.out.println("\n[5/6] Running Optimized Concurrent (4 Threads)...");
        OptimizedBenchmark opt4 = new OptimizedBenchmark(4, duration, true);
        BenchmarkMetrics mOpt4 = opt4.runBenchmark();

        // 6. Optimized Concurrent (8 threads)
        System.out.println("\n[6/6] Running Optimized Concurrent (8 Threads)...");
        OptimizedBenchmark opt8 = new OptimizedBenchmark(8, duration, true);
        BenchmarkMetrics mOpt8 = opt8.runBenchmark();

        double serialTps = mSerial.getThroughput();

        System.out.println("\n==================================================================================================");
        System.out.println("                                FINAL BENCHMARK COMPARISON TABLE                                  ");
        System.out.println("==================================================================================================");
        System.out.printf("%-30s | %-8s | %-15s | %-15s | %-10s%n",
                "Execution Setup", "Threads", "Total Commits", "Throughput (tx/s)", "Speedup");
        System.out.println("--------------------------------------------------------------------------------------------------");
        printRow("Serial Baseline (Unoptimized)", 1, mSerial, serialTps);
        printRow("Baseline Concurrent", 4, mBase4, serialTps);
        printRow("Baseline Concurrent", 8, mBase8, serialTps);
        printRow("Optimized Concurrent", 2, mOpt2, serialTps);
        printRow("Optimized Concurrent", 4, mOpt4, serialTps);
        printRow("Optimized Concurrent", 8, mOpt8, serialTps);
        System.out.println("==================================================================================================");
    }

    private static void printRow(String name, int threads, BenchmarkMetrics m, double baselineTps) {
        double tps = m.getThroughput();
        double speedup = tps / baselineTps;
        System.out.printf("%-30s | %-8d | %-15d | %-15.2f | %-9.2fx%n",
                name, threads, m.committedTotal.get(), tps, speedup);
    }
}
