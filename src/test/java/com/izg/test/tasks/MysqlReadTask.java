package com.izg.test.tasks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;

public class MysqlReadTask implements Callable<Integer>{
    private final List<String> list;
    private final Connection conn;


    public MysqlReadTask( Connection conn, List<String> list) {
    	this.conn = conn;
        this.list = list;
    }

    public Integer call() throws Exception {

    	PreparedStatement prepareStatement = conn.prepareStatement("SELECT * FROM testtable WHERE id = ?");

    	for (String id : list) {
    		prepareStatement.setString(1, id);
    		ResultSet resultSet = prepareStatement.executeQuery();
    		ArrayList<String> result = new ArrayList<String>();
    		while (resultSet.next()) {
    			result.add(resultSet.getString("json"));
    		}

        	Assert.assertNotNull(result);
        	Assert.assertEquals(1, result.size());
		}
    	return Integer.valueOf(1);
    }
}