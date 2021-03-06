package com.izg.test.tasks;

import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class RedisReadTask implements Callable<Integer>{
    private final List<String> list;


    public RedisReadTask( List<String> list) {
        this.list = list;
    }

    public Integer call() throws Exception {

    	Jedis jedis = new Jedis("localhost", Protocol.DEFAULT_PORT);
		jedis.connect();

    	for (String id : list) {
    		String object = jedis.get(id);
        	Assert.assertNotNull(object);
		}
    	jedis.disconnect();
    	return Integer.valueOf(1);
    }
}