package com.izg.test.tasks;

import static com.googlecode.objectify.ObjectifyService.ofy;

import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;

import com.igz.performance.datastore.DatastoreObject;

public class DatastoreReadTask implements Callable<Integer> {
	private final List<String> list;

	public DatastoreReadTask(List<String> list) {

		this.list = list;
	}

	public Integer call() throws Exception {

		for (String id : list) {
			DatastoreObject object = ofy().load().type(DatastoreObject.class).id(id).get();

			Assert.assertNotNull(object);
			ofy().clear();
		}
		return Integer.valueOf(1);
	}
}