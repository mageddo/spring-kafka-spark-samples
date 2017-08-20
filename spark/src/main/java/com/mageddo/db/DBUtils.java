package com.mageddo.db;


import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.SQLException;

public class DBUtils {

	private static DataSource datasource;

	static {

		PoolProperties p = new PoolProperties();
		p.setUrl("jdbc:mysql://localhost:3306/mysql");
		p.setDriverClassName("com.mysql.jdbc.Driver");
		p.setUsername("sa");
		p.setPassword("");
		p.setUrl("jdbc:h2:mem:test");
		p.setDriverClassName("org.h2.Driver");
		p.setMaxActive(5);
		p.setInitialSize(1);
//		p.setMinIdle(1);
		p.setMaxWait(1);

		datasource = new DataSource(p);
	}

	public static Connection getConnection(){
		try {
			return datasource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static DataSource getDatasource() {
		return datasource;
	}
}
