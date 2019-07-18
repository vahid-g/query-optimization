package bandits;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnection implements Closeable {

	private Properties config;
	private Connection connection;

	public DatabaseConnection() throws IOException, SQLException {
		config = new Properties();
		try (InputStream in = DatabaseConnection.class.getResourceAsStream("config.properties")) {
			config.load(in);
		}
		Properties connectionProps = new Properties();
		connectionProps.put("user", config.get("username"));
		connectionProps.put("password", config.get("password"));
		try {
			connection = DriverManager.getConnection(config.getProperty("wiki-db"), connectionProps);
		} catch (SQLException e) {
			throw e;
		}
	}

	private void closeConnection() throws SQLException {
		if (connection != null) {
			System.err.println("Closing the databse connection..");
			connection.close();
		}
	}

	@Override
	public void close() throws IOException {
		try {
			closeConnection();
		} catch (SQLException e) {
			throw new IOException();
		}

	}

	public Connection getConnection() {
		return connection;
	}

}
