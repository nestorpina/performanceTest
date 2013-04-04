package com.igz.performance.queues;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;

public class RabbitMQ {

	protected static final boolean QUEUE_CONFIG_DURABLE = true;
	protected static final boolean QUEUE_CONFIG_EXCLUSIVE = false;
	protected static final boolean QUEUE_CONFIG_AUTODELETE = false;

	protected String queue_name;
	
	protected boolean debug = false;
	
	public RabbitMQ() {}
	
	public RabbitMQ(String queue_name) {
		this.queue_name = queue_name;
	}

	public int getPendingMessages() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection;
		int messageCount = -1;
		try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();

			DeclareOk queueDeclare = channel.queueDeclare(queue_name, QUEUE_CONFIG_DURABLE, QUEUE_CONFIG_EXCLUSIVE,
					QUEUE_CONFIG_AUTODELETE, null);
			messageCount = queueDeclare.getMessageCount();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return messageCount;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
}
