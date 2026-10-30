import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Teil 3 der Hausuebung: wie Blocking.java, aber eine Transaktion, die vom DBMS
 * (z.B. wegen eines Deadlocks) abgebrochen wird, wird vom Client so lange neu
 * gestartet, bis sie committet.
 *
 * Aufruf (optional mit Parametern):
 *   java -cp ".;postgresql-42.7.14.jar" RestartBlocking [tasks] [queriesPerTask] [threads]
 *   z.B. java -cp ".;postgresql-42.7.14.jar" RestartBlocking 50 5 8
 *
 * Basiert auf Blocking.java von Martin Schaeler (martin.schaeler@plus.ac.at).
 */
public class RestartBlocking {
	public static int num_taks = 30;
	public static int num_queries_per_task = 3;
	public static int num_threads = 4;

	// SQLStates, bei denen ein Neustart sinnvoll ist
	static final String DEADLOCK_DETECTED     = "40P01"; // PostgreSQL: deadlock_detected
	static final String SERIALIZATION_FAILURE = "40001"; // serialization_failure

	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	AtomicInteger total_restarts;

	public RestartBlocking() {
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

	/** Darf diese Transaktion nach dem Fehler einfach neu gestartet werden? */
	static boolean isRetryable(SQLException e) {
		String state = e.getSQLState();
		return DEADLOCK_DETECTED.equals(state) || SERIALIZATION_FAILURE.equals(state);
	}

	static void rollbackQuietly(Connection con) {
		try {
			con.rollback();
		} catch (SQLException ignored) {
			// Verbindung evtl. schon kaputt - dann gibt es nichts mehr zurueckzurollen
		}
	}

	public static void main(String[] args) {
		if (args.length > 0) num_taks = Integer.parseInt(args[0]);
		if (args.length > 1) num_queries_per_task = Integer.parseInt(args[1]);
		if (args.length > 2) num_threads = Integer.parseInt(args[2]);
		System.out.println("Tasks: " + num_taks + ", queries per task: " + num_queries_per_task + ", threads: " + num_threads);

		// Create DB
		RestartBlocking b = new RestartBlocking();
		b.cerateDB();

		// Create tasks and their queries
		ArrayList<RestartBlocking.Blocker> my_tasks = new ArrayList<RestartBlocking.Blocker>(num_taks);
		for (int task = 0; task < num_taks; task++) {
			my_tasks.add(b.new Blocker(task));
		}

		// Execute tasks
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);
		double start, stop;
		start = System.currentTimeMillis();
		for (Blocker task : my_tasks) {
			executor.execute(task);
		}
		executor.shutdown();

		// wait for the tasks to finish
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

	/**
	 * Ein Task, der viele Schreibsperren lange haelt. Wird seine Transaktion
	 * abgebrochen, wird sie zurueckgerollt und komplett neu ausgefuehrt.
	 */
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

		@Override
		public void run() {
			// eigene Verbindung pro Task, wird fuer alle Versuche wiederverwendet
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			int attempt = 0;
			boolean committed = false;

			try {
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				PreparedStatement ps = con.prepareStatement(
						"UPDATE blocking_data SET data_value = ?,  modified_by = " + task + " WHERE data_value >= ? AND data_value <= ?;",
						ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

				while (!committed) {
					attempt++;
					try {
						// die komplette Transaktion: alle Updates + Commit
						for (int q = 0; q < num_queries_per_task; q++) {
							ps.setDouble(1, (float) (rand.nextDouble() * max_value));
							ps.setDouble(2, start_range[q]);
							ps.setDouble(3, stop_range[q]);
							ps.execute();
						}
						con.commit();
						committed = true;
					} catch (SQLException e) {
						// Transaktion wurde abgebrochen -> zuruecksetzen
						rollbackQuietly(con);
						if (isRetryable(e)) {
							total_restarts.incrementAndGet();
							System.err.println("Task " + task + " aborted in attempt " + attempt
									+ " (SQLState " + e.getSQLState() + ") -> restarting");
						} else {
							throw e; // anderer Fehler: Neustart bringt nichts
						}
					}
				}

				ps.close();
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
