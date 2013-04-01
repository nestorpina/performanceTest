package com.izg.test.tasks;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Assert;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class RedisReadTask implements Callable<Integer>{
    private final List<String> list;
    private final String[] fields;


    public RedisReadTask( List<String> list, Set<String> fields) {
    	this.fields = fields.toArray(new String[fields.size()]);
        this.list = list;
    }

    public Integer call() throws Exception {

    	Jedis jedis = new Jedis("localhost", Protocol.DEFAULT_PORT);
		jedis.connect();

    	for (String id : list) {
//    		String object = jedis.get(id);
    		List<String> object = jedis.hmget(id, fields);
        	Assert.assertNotNull(object);
		}
    	jedis.disconnect();
    	return Integer.valueOf(1);
    }
}