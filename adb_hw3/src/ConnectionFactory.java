import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// ConnectionFactory provides JDBC connections to PostgreSQL.
// Credentials are read dynamically from the private .env file
// or system environment variables, keeping sensitive passwords out of source control.
public class ConnectionFactory {

    public static final String POSTGRESQL = "POSTGRESQL";

    private static final Map<String, String> ENV_PROPERTIES = loadEnvFile();

    private static Map<String, String> loadEnvFile() {
        Map<String, String> map = new HashMap<>();
        File[] candidatePaths = {
            new File(".env"),
            new File("adb_hw3/.env"),
            new File("../.env")
        };

        for (File file : candidatePaths) {
            if (file.exists() && file.isFile()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || line.startsWith("#")) {
                            continue;
                        }
                        int sepIndex = line.indexOf('=');
                        if (sepIndex > 0) {
                            String key = line.substring(0, sepIndex).trim();
                            String value = line.substring(sepIndex + 1).trim();
                            if ((value.startsWith("\"") && value.endsWith("\"")) ||
                                (value.startsWith("'") && value.endsWith("'"))) {
                                value = value.substring(1, value.length() - 1);
                            }
                            map.put(key, value);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Warning reading .env file: " + e.getMessage());
                }
                break;
            }
        }
        return map;
    }

    private static String getEnv(String key, String defaultValue) {
        String systemVal = System.getenv(key);
        if (systemVal != null && !systemVal.isEmpty()) {
            return systemVal;
        }
        String fileVal = ENV_PROPERTIES.get(key);
        if (fileVal != null && !fileVal.isEmpty()) {
            return fileVal;
        }
        return defaultValue;
    }

    // Creates and returns a Connection to the PostgreSQL database using settings from .env.
    public static Connection getDefaultParameterConnection(String dbType) {
        if (!POSTGRESQL.equalsIgnoreCase(dbType)) {
            throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }

        String host = getEnv("PGHOST", "localhost");
        String port = getEnv("PGPORT", "5432");
        String dbName = getEnv("PGDATABASE", "postgres");
        String user = getEnv("PGUSER", "postgres");
        String password = getEnv("PGPASSWORD", "");

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;

        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        try {
            return DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            throw new RuntimeException("Could not connect to PostgreSQL database: " + url + " - " + e.getMessage(), e);
        }
    }
}
