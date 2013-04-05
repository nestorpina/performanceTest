package com.igz.performance.queues;

import java.io.IOException;

import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.queues.RabbitMQTest.OperationType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQConsumer extends RabbitMQ implements Runnable {

	private static final int PREFETCH_COUNT = 1;
	private String name;
	private DatabaseDAO dao;

	public RabbitMQConsumer(String name, String queue_name, DatabaseDAO dao) {
		this.name = name;
		this.queue_name = queue_name;
		this.dao = dao;

		dao.init();
	}
	
	public DatabaseDAO getDao() {
		return dao;
	}

	public void setDao(DatabaseDAO dao) {
		this.dao = dao;
	}

	public void run() {

		Channel channel = null;
		try {
			
			connection = getConnection();
			channel = connection.createChannel();

			channel.queueDeclare(queue_name, QUEUE_CONFIG_DURABLE, QUEUE_CONFIG_EXCLUSIVE, QUEUE_CONFIG_AUTODELETE, null);
			channel.basicQos(PREFETCH_COUNT);

			if (debug) {
				System.out.println(" [" + name + "] Consumer started");
			}

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queue_name, false, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());

				// Proccess message
				if (debug) {
					System.out.println(" [" + name + "] Received '" + message.substring(0, 100) + "'" + (message.length()>100?"...":""));
				}
				String[] msgSplitted = message.split("\\"+SEPARATOR);
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

				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ShutdownSignalException e) {
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			if(debug) {
				System.out.println(String.format("Queue [%s] deleted. Consumer [%s] cancelled.",queue_name,name));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			try {
				channel.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
