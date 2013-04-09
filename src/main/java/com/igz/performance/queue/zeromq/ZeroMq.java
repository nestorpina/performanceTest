package com.igz.performance.queue.zeromq;

import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.queue.AbstractQueue;
import com.igz.performance.queue.interfaces.Consumer;
import com.igz.performance.queue.interfaces.Producer;
import com.igz.performance.queue.interfaces.Queue;

public class ZeroMq extends AbstractQueue implements Queue {


	public ZeroMq(String queueName) {
		super(queueName);
	}

	public int getPendingMessages() {
		return ZeroMQResultsConsumer.getExpectedResults() - ZeroMQResultsConsumer.getProcessedResults();
	}

	public void deleteQueue() {
		// TODO Auto-generated method stub

	}

	public void startResultsConsumer(int expectedResults) {


	}
	
	public Consumer createConsumer(String name, DatabaseDAO databaseDAO) {
		return new ZeroMQConsumer(name, queueName, databaseDAO);
	}
	
	public Producer createProducer(String name, int numberOfRequests) {
		return new ZeroMQProducer(name, queueName, numberOfRequests);
	}

}
