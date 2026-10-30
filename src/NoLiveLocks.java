import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Teil 4 der Hausuebung: Livelocks / Starvation vermeiden.
 *
 * Problem in RestartBlocking: Tasks sperren Zeilen in zufaelliger Reihenfolge
 * (je nachdem, wie das UPDATE die Tabelle durchlaeuft und welche Wertebereiche
 * es trifft). Dadurch entstehen Zyklen im Wait-for-Graph (Deadlocks). Ein
 * abgebrochener Task startet sofort neu und laeuft wieder in dieselben Zyklen
 * -> Livelock (z.B. 60 Tasks / 5 Queries / 8 Threads: 1018 Restarts, 735 s).
 *
 * Loesung: Konservatives Sperren mit globaler Sperr-Reihenfolge
 * (Predeclaration + Lock Ordering, vgl. Folien "Deadlock prevention").
 *  1. Zu Beginn der Transaktion sperrt der Task ALLE Zeilen, die er mit seinen
 *     Updates aendern wird, in EINEM Statement, aufsteigend nach data_id:
 *       SELECT data_id ... WHERE <Bereich 1> OR <Bereich 2> ...
 *       ORDER BY data_id FOR UPDATE
 *  2. Danach fuehrt er seine Updates aus, beschraenkt auf genau diese Zeilen
 *     (data_id = ANY(gesperrte Ids)). Es kommen also keine neuen Sperren dazu.
 *  3. Commit gibt alle Sperren frei.
 * Da alle Transaktionen ihre Sperren in derselben Reihenfolge (aufsteigende
 * data_id) anfordern und spaeter keine weiteren Sperren brauchen, kann kein
 * Zyklus im Wait-for-Graph entstehen -> keine Deadlocks -> keine Abbrueche
 * -> keine Livelocks/Starvation. Wartende Transaktionen stehen in der
 * Warteschlange der Sperre und kommen der Reihe nach dran.
 *
 * Sicherheitsnetz: Sollte das DBMS trotzdem abbrechen (40P01/40001), wird
 * mit zufaelligem, exponentiell wachsendem Backoff neu gestartet.
 *
 * Aufruf: java -cp ".;postgresql-42.7.14.jar" NoLiveLocks [tasks] [queriesPerTask] [threads]
 *
 * Basiert auf Blocking.java von Martin Schaeler (martin.schaeler@plus.ac.at).
 */
public class NoLiveLocks {
	public static int num_taks = 30;
	public static int num_queries_per_task = 3;
	public static int num_threads = 4;

	static final String DEADLOCK_DETECTED     = "40P01";
	static final String SERIALIZATION_FAILURE = "40001";
	static final long   MAX_BACKOFF_MS        = 1000;

	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	AtomicInteger total_restarts;

	public NoLiveLocks() {
		tasks_finished = new AtomicInteger(0);
		total_restarts = new AtomicInteger(0);
	}

	void executeSQL(String sql, Connection con) {
		java.sql.Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			System.out.println("Executing " + sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	void cerateDB() {
		try {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

			PreparedStatement ps;
			con.setAutoCommit(false);
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

			for (int i = 0; i < num_tuple; i++) {
				ps.setInt(1, i);
				ps.setDouble(2, rand.nextDouble() * max_value);
				ps.setInt(3, 0);
				ps.addBatch();
				ps.clearParameters();
			}

			int[] results = ps.executeBatch();
			con.commit();
			con.close();
			System.out.println("Inserted " + results.length + " tuples");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	static boolean isRetryable(SQLException e) {
		String state = e.getSQLState();
		return DEADLOCK_DETECTED.equals(state) || SERIALIZATION_FAILURE.equals(state);
	}

	static void rollbackQuietly(Connection con) {
		try {
			con.rollback();
		} catch (SQLException ignored) {
		}
	}

	/** Zufaellige Wartezeit, die mit jedem Fehlversuch waechst (nur Sicherheitsnetz). */
	static void backoff(int attempt) {
		long cap = Math.min(MAX_BACKOFF_MS, 10L << Math.min(attempt, 10));
		try {
			Thread.sleep(ThreadLocalRandom.current().nextLong(cap + 1));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public static void main(String[] args) {
		if (args.length > 0) num_taks = Integer.parseInt(args[0]);
		if (args.length > 1) num_queries_per_task = Integer.parseInt(args[1]);
		if (args.length > 2) num_threads = Integer.parseInt(args[2]);
		System.out.println("Tasks: " + num_taks + ", queries per task: " + num_queries_per_task + ", threads: " + num_threads);

		NoLiveLocks b = new NoLiveLocks();
		b.cerateDB();

		ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<NoLiveLocks.Blocker>(num_taks);
		for (int task = 0; task < num_taks; task++) {
			my_tasks.add(b.new Blocker(task));
		}

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);
		double start, stop;
		start = System.currentTimeMillis();
		for (Blocker task : my_tasks) {
			executor.execute(task);
		}
		executor.shutdown();

		while (!executor.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		stop = System.currentTimeMillis();
		System.out.println("[DONE] Task execution after roughly " + (stop - start) + " ms finished: "
				+ b.tasks_finished + " of " + num_taks + ", restarts: " + b.total_restarts);
	}

	public class Blocker implements Runnable {
		final int task;
		final double[] start_range;
		final double[] stop_range;

		public Blocker(int id) {
			this.task = id;
			this.start_range = new double[num_queries_per_task];
			this.stop_range = new double[num_queries_per_task];
			for (int q = 0; q < num_queries_per_task; q++) {
				stop_range[q] = rand.nextDouble() * max_value;
				start_range[q] = rand.nextDouble() * stop_range[q];
			}
		}

		/** SELECT, das alle Zeilen aller Bereiche dieses Tasks sortiert nach data_id sperrt. */
		String buildLockQuery() {
			StringBuilder sb = new StringBuilder("SELECT data_id FROM blocking_data WHERE ");
			for (int q = 0; q < num_queries_per_task; q++) {
				if (q > 0) sb.append(" OR ");
				sb.append("(data_value >= ? AND data_value <= ?)");
			}
			sb.append(" ORDER BY data_id FOR UPDATE;");
			return sb.toString();
		}

		@Override
		public void run() {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			int attempt = 0;
			boolean committed = false;

			try {
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

				PreparedStatement lockPs = con.prepareStatement(buildLockQuery());
				PreparedStatement updatePs = con.prepareStatement(
						"UPDATE blocking_data SET data_value = ?,  modified_by = " + task
						+ " WHERE data_value >= ? AND data_value <= ? AND data_id = ANY(?);");

				while (!committed) {
					attempt++;
					try {
						// Phase 1: alle benoetigten Zeilen in globaler Reihenfolge sperren
						for (int q = 0; q < num_queries_per_task; q++) {
							lockPs.setDouble(2 * q + 1, start_range[q]);
							lockPs.setDouble(2 * q + 2, stop_range[q]);
						}
						List<Integer> ids = new ArrayList<Integer>();
						ResultSet rs = lockPs.executeQuery();
						while (rs.next()) {
							ids.add(rs.getInt(1));
						}
						rs.close();
						Array lockedIds = con.createArrayOf("integer", ids.toArray());

						// Phase 2: Updates nur auf den bereits gesperrten Zeilen
						for (int q = 0; q < num_queries_per_task; q++) {
							updatePs.setDouble(1, (float) (rand.nextDouble() * max_value));
							updatePs.setDouble(2, start_range[q]);
							updatePs.setDouble(3, stop_range[q]);
							updatePs.setArray(4, lockedIds);
							updatePs.execute();
						}

						// Phase 3: Commit gibt alle Sperren frei
						con.commit();
						lockedIds.free();
						committed = true;
					} catch (SQLException e) {
						rollbackQuietly(con);
						if (isRetryable(e)) {
							total_restarts.incrementAndGet();
							System.err.println("Task " + task + " aborted in attempt " + attempt
									+ " (SQLState " + e.getSQLState() + ") -> backoff + restart");
							backoff(attempt);
						} else {
							throw e;
						}
					}
				}

				lockPs.close();
				updatePs.close();
				int num_finished = tasks_finished.incrementAndGet();
				System.out.println("Done task " + task + " after " + attempt + " attempt(s). I am finisher number " + num_finished);
			} catch (SQLException e) {
				System.err.println("Task " + task + " failed permanently: " + e.getMessage());
			} finally {
				try {
					con.close();
				} catch (SQLException ignored) {
				}
			}
		}
	}
}
