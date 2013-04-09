package com.igz.performance.queue.interfaces;

import com.igz.performance.database.DatabaseDAO;

public interface Queue {

	public abstract int getPendingMessages();

	public abstract void deleteQueue();

	public abstract boolean isDebug();

	public abstract void setDebug(boolean debug);
	
	public Consumer createConsumer(String name, DatabaseDAO databaseDAO);
	
	public Producer createProducer(String name, int numberOfRequests);

}