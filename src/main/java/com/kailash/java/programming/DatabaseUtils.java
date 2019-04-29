package com.kailash.java.programming;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

public class DatabaseUtils {

	private static HikariDataSource dataSource;
	private static StringBuilder preparedStatementQuery;
	private static Properties properties;

	static {
		properties = new Properties();
		String dbProperties = System.getProperty("databaseConfig");
		try {
			properties.load(new FileInputStream(dbProperties));
		} catch (IOException e) {
			e.printStackTrace();
		}
		HikariConfig config = new HikariConfig();
		config.setMaximumPoolSize(10);
		config.setDriverClassName(properties.getProperty("db.driver"));
		config.setJdbcUrl(properties.getProperty("db.url"));
		config.setUsername(properties.getProperty("db.username"));
		config.setPassword(properties.getProperty("db.password"));
		dataSource = new HikariDataSource(config);

	}

	public static Connection getConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void createTable(List<String> columns) {
		try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
			StringBuilder tableQuery = new StringBuilder();
			tableQuery.append("CREATE TABLE IF NOT EXISTS CSVIMPORTEDDATA (");
			// tableQuery.append("ID int NOT NULL AUTO_INCREMENT, ");
			// tableQuery.append("( ");

			for (int i = 0; i < columns.size(); i++) {
				tableQuery.append(columns.get(i)).append(" ").append(" varchar(255)");
				if (i < columns.size()-1) {
					tableQuery.append(",");
				}
			}
			// tableQuery.append(" PRIMARY KEY (ID))");
			tableQuery.append(")");
			System.out.println("Table create with query  :" + tableQuery.toString());
			stmt.executeUpdate(tableQuery.toString());

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static void processRecords(List<String> dataList) {
		try (Connection con = getConnection();
				PreparedStatement preparedStatement = con.prepareStatement(getPreparedStatement())) {
			for (int i = 0; i < dataList.size(); i++) {
				preparedStatement.setString(i + 1, dataList.get(i));
			}
			preparedStatement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void createPreparedStatement(List<String> columns) {
		preparedStatementQuery = new StringBuilder();
		preparedStatementQuery.append("INSERT INTO CSVIMPORTEDDATA (");
		for (int i = 0; i < columns.size(); i++) {
			preparedStatementQuery.append(columns.get(i)).append(",");
		}
		preparedStatementQuery.deleteCharAt(preparedStatementQuery.length() - 1);
		preparedStatementQuery.append(") VALUES (");
		for (int i = 0; i < columns.size(); i++) {
			preparedStatementQuery.append("?").append(",");
		}
		preparedStatementQuery.deleteCharAt(preparedStatementQuery.length() - 1);
		preparedStatementQuery.append(")");
	}

	public static String getPreparedStatement() {
		return preparedStatementQuery.toString();
	}

}
