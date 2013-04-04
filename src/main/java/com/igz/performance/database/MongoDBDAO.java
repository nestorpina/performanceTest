package com.igz.performance.database;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class MongoDBDAO implements DatabaseDAO {

	private static final String COLLECTION = "test";
	private static final String DATABASE = "mydb";
	private static final String HOST = "localhost";
	private static final int PORT = 27017;

	private MongoClient mongoClient;
	private DBCollection collection;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#init()
	 */
	public void init() {

		try {
			mongoClient = new MongoClient(HOST, PORT);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return;
		}
		DB db = mongoClient.getDB(DATABASE);
		collection = db.getCollection(COLLECTION);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#insert()
	 */
	public String insert(String id, String json) {
		// We ignore the id in mongo and let mongo use its internal id generator
		DBObject object = (DBObject) JSON.parse(json);
		object.put("_id", id);
		collection.insert(object);
		return id;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#select()
	 */
	public int select(String id) {

		DBObject found = collection.findOne(new BasicDBObject().append("_id", id));
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
		collection.drop();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#count()
	 */
	public int count() {
		long count = collection.count();
		return Long.valueOf(count).intValue();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.database.DatabaseDAO#close()
	 */
	public void close() throws Throwable {
		mongoClient.close();
	}
}
