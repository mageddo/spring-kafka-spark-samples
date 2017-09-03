package com.mageddo.spark.students;

import org.apache.tomcat.jdbc.pool.DataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DBUtils {

	private static DataSource datasource;

	static {

		/*
			Documentação das propriedades do pool https://tomcat.apache.org/tomcat-7.0-doc/jdbc-pool.html
		 */
		final Properties connProp = new Properties();
		connProp.setProperty("username", "sa");
		connProp.setProperty("password", "");
		connProp.setProperty("defaultAutoCommit", "false");
		connProp.setProperty("defaultTransactionIsolation", "READ_COMMITTED");
		connProp.setProperty("url", "jdbc:hsqldb:mem:testdb");
		connProp.setProperty("driverClassName", "org.hsqldb.jdbcDriver");
		try {
			datasource = new DataSourceFactory().createDataSource(connProp);
		} catch (Exception e) {
			throw new RuntimeException("Can not create pool", e);
		}

	}

	public static Connection getConnection() throws SQLException {
		return datasource.getConnection();
	}
}
