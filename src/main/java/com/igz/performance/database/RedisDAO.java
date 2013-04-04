package com.igz.performance.database;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class RedisDAO implements DatabaseDAO {

	private static final String HOST = "localhost";
	private static final int PORT = Protocol.DEFAULT_PORT;

	private Jedis jedis;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#init()
	 */
	public void init() {
		jedis = new Jedis(HOST, PORT);
		jedis.connect();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#insert()
	 */
	public String insert(String id, String json) {
		jedis.set(id, json);
		return id;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#select()
	 */
	public int select(String id) {
		String found = jedis.get(id);
		if (found == null) {
			System.err.println("RESULT NOT FOUND LOOKING BY ID:" + id);
		}

		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#removeAll()
	 */
	public void removeAll() {
		jedis.flushDB();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#count()
	 */
	public int count() {
		Long count = jedis.dbSize();
		return count.intValue();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#close()
	 */
	public void close() throws Throwable {
		jedis.disconnect();
	}
}
