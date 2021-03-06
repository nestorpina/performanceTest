package com.igz.performance.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public abstract class JdbcGenericDAO implements DatabaseDAO {

	private Connection conn;

	protected abstract Connection getConnection();
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#init()
	 */
	public void init() {
			conn = getConnection();
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#insert()
	 */
	public String insert(String id, String json) {
		try {
			PreparedStatement preparedStatement = conn.prepareStatement("insert into  testtable values (?, ?)");
			preparedStatement.setString(1, (String) id);
			preparedStatement.setString(2, json);
			preparedStatement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return id;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#select()
	 */
	public int select(String id) {

		PreparedStatement preparedStatement = null;
		ArrayList<String> result = new ArrayList<String>();
		try {
			preparedStatement = conn.prepareStatement("SELECT * FROM testtable WHERE id = ?");
			preparedStatement.setString(1, (String) id);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				result.add(resultSet.getString("json"));
			}
			resultSet.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (result.size() == 0) {
			System.err.println("RESULT NOT FOUND LOOKING BY ID:" + id);
		} else if (result.size() > 1) {
			System.err.println("FOUND MORE THAN ONE RESULT LOOKING BY ID:" + id + " size:" + result.size());
		}

		return result.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#removeAll()
	 */
	public void removeAll() {
		PreparedStatement preparedStatement;
		try {
			preparedStatement = conn.prepareStatement("delete from testtable;");
			preparedStatement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#count()
	 */
	public int count() {
		int result = -1;
		try {
			PreparedStatement preparedStatement = conn.prepareStatement("select count(*) from  testtable");
			preparedStatement.executeQuery();
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				result = resultSet.getInt(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#close()
	 */
	public void close() throws Throwable {
		conn.close();
	}
}
