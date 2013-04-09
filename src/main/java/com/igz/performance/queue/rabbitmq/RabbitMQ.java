package com.igz.performance.queue.rabbitmq;

import java.io.IOException;

import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.queue.AbstractQueue;
import com.igz.performance.queue.interfaces.Consumer;
import com.igz.performance.queue.interfaces.Producer;
import com.igz.performance.queue.interfaces.Queue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;

public class RabbitMQ extends AbstractQueue implements Queue {

	protected static final boolean QUEUE_CONFIG_DURABLE = true;
	protected static final boolean QUEUE_CONFIG_EXCLUSIVE = false;
	protected static final boolean QUEUE_CONFIG_AUTODELETE = false;

	protected Connection connection;

	public RabbitMQ(String queueName) {
		super(queueName);
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Queue#getPendingMessages()
	 */
	public int getPendingMessages() {

		int messageCount = -1;
		Channel channel = null;
		try {
			channel = getConnection().createChannel();

			DeclareOk queueDeclare = channel.queueDeclare(queueName, QUEUE_CONFIG_DURABLE, QUEUE_CONFIG_EXCLUSIVE,
					QUEUE_CONFIG_AUTODELETE, null);
			messageCount = queueDeclare.getMessageCount();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return messageCount;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Queue#deleteQueue()
	 */
	public void deleteQueue() {
		if (debug) {
			System.out.println("Deleting queue: " + queueName);
		}
		Channel channel = null;
		try {
			channel = getConnection().createChannel();
			channel.queueDelete(queueName);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				channel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	protected Connection getConnection() throws IOException {
		if (connection == null || !connection.isOpen()) {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(HOST);
			connection = factory.newConnection();
		}
		return connection;
	}

	public Consumer createConsumer(String name, DatabaseDAO databaseDAO) {
		return new RabbitMQConsumer(name, queueName, databaseDAO);

	}

	public Producer createProducer(String name, int numberOfRequests) {
		return new RabbitMQProducer(name, queueName, numberOfRequests);
	}
}
