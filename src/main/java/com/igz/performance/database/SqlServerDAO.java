package com.igz.performance.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlServerDAO extends JdbcGenericDAO implements DatabaseDAO {

	@Override
	protected Connection getConnection() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	    	return DriverManager.getConnection("jdbc:sqlserver://localhost;integratedSecurity=true");
    	} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}
