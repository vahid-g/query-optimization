package bandits;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseManager {

	public static Connection createConnection() throws SQLException, IOException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		Properties config = new Properties();
		try (InputStream in = DatabaseManager.class.getResourceAsStream("config.properties")) {
			config.load(in);
		}
		connectionProps.put("user", config.get("username"));
		connectionProps.put("password", config.get("password"));
		conn = DriverManager.getConnection(config.getProperty("wiki-db"), connectionProps);
		conn.setAutoCommit(false);
		System.out.println("Database connection created.");
		return conn;
	}
}
