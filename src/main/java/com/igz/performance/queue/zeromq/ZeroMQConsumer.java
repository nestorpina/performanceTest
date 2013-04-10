package com.igz.performance.queue.zeromq;

import org.apache.commons.lang.StringUtils;
import org.zeromq.ZMQ;

import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.queue.interfaces.Consumer;

public class ZeroMQConsumer extends ZeroMq implements Consumer {

	private String name;
	private DatabaseDAO dao;

	public ZeroMQConsumer(String name, String queueName, DatabaseDAO databaseDAO) {
		super(queueName);
		this.name = name;
		this.dao = databaseDAO;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Consumer#getDao()
	 */
	public DatabaseDAO getDao() {
		return dao;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Consumer#setDao(com.igz.performance.database.DatabaseDAO)
	 */
	public void setDao(DatabaseDAO dao) {
		this.dao = dao;
	}

	public void run() {
		ZMQ.Context context = ZMQ.context(1);

		ZMQ.Socket consumer = context.socket(ZMQ.PULL);
		consumer.connect("tcp://localhost:5556");

		ZMQ.Socket resultProducer = context.socket(ZMQ.PUSH);
		resultProducer.connect("tcp://localhost:5557");

		if (debug) {
			System.out.println(" [" + name + "] Consumer started");
		}

		while (!Thread.currentThread().isInterrupted()) {

			// Use trim to remove the tailing '0' character
			String message = consumer.recvStr(0).trim();

			// Proccess message
			if (debug) {
				System.out.println(" [" + name + "] Received '" + StringUtils.abbreviate(message, 100) + "'");
			}
			String[] msgSplitted = message.split("\\" + SEPARATOR);
			OperationType operation = OperationType.valueOf(msgSplitted[0]);
			String id = msgSplitted[1];

			// Execute operation
			if (operation.equals(OperationType.INSERT)) {
				String json = msgSplitted[2];
				dao.insert(id, json);
			} else if (operation.equals(OperationType.SELECT)) {
				dao.select(id);
			} else {
				System.err.println("Unssuported message type : " + operation);
			}
			resultProducer.send("OK");

		}

		resultProducer.close();
		consumer.close();
		context.term();

	}

}
