package com.igz.performance.queues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.igz.performance.queues.RabbitMQTest.OperationType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class RabbitMQProducer extends RabbitMQ implements Callable<List<Object>> {

	private String name;
	private int numberOfRequests;
	private OperationType operation;
	private Channel channel;
	private List<Object> idsToSelect;

	public RabbitMQProducer(String name, String queue_name, int numberOfRequests) {
		this.name = name;
		this.queue_name = queue_name;
		this.numberOfRequests = numberOfRequests;
	}

	public List<Object> getIdsToSelect() {
		return idsToSelect;
	}

	public void setIdsToSelect(List<Object> idsToSelect) {
		this.idsToSelect = idsToSelect;
	}

	public OperationType getOperation() {
		return operation;
	}

	public void setOperation(OperationType operation) {
		this.operation = operation;
	}

	public List<Object> call() throws IOException {

		List<Object> ids = new ArrayList<Object>();

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection;
		connection = factory.newConnection();
		channel = connection.createChannel();

		channel.queueDeclare(queue_name, QUEUE_CONFIG_DURABLE, QUEUE_CONFIG_EXCLUSIVE, QUEUE_CONFIG_AUTODELETE, null);

		if (operation == OperationType.INSERT) {
			ids = sendInserts();
		} else if (operation == OperationType.SELECT) {
			ids = sendSelects();
		}

		channel.close();
		connection.close();

		return ids;

	}

	private List<Object> sendSelects() throws IOException {
		List<Object> ids = new ArrayList<Object>();
		for (Object id : idsToSelect) {
			String message = operation + "|" + id;
			channel.basicPublish("", queue_name, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			if(debug) {
				System.out.println(" [" + name + "] Sent '" + message + "'");
			}
		}
		return ids;
	}

	private List<Object> sendInserts() throws IOException {
		List<Object> ids = new ArrayList<Object>();
		for (int i = 0; i < numberOfRequests; i++) {
			String id = UUID.randomUUID().toString();
			ids.add(id);
			String message = operation + "|" + id;
			channel.basicPublish("", queue_name, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			if(debug) {
				System.out.println(" [" + name + "] Sent '" + message + "'");
			}
		}
		return ids;
	}

}