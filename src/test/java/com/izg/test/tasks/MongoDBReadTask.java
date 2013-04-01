package com.izg.test.tasks;

import java.util.List;
import java.util.concurrent.Callable;

import org.bson.types.ObjectId;
import org.junit.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoDBReadTask implements Callable<Integer>{
    private final List<ObjectId> list;
    DBCollection collection;

    public MongoDBReadTask(DBCollection collection, List<ObjectId> list) {
    	this.collection = collection;
        this.list = list;
    }

    public Integer call() throws Exception {

    	for (ObjectId objectId : list) {
        	DBObject findOne = collection.findOne(new BasicDBObject().append("_id", objectId));
        	Assert.assertNotNull(findOne);
		}
    	return Integer.valueOf(1);
    }
}