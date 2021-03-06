package com.igz.performance.queue.rabbitmq;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.queue.interfaces.Consumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQConsumer extends RabbitMQ implements Consumer {

	private static final int PREFETCH_COUNT = 1;
	private String name;
	private DatabaseDAO dao;

	public RabbitMQConsumer(String name, String queueName, DatabaseDAO dao) {
		super(queueName);
		this.name = name;
		this.dao = dao;
	}
	
	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Consumer#getDao()
	 */
	public DatabaseDAO getDao() {
		return dao;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Consumer#setDao(com.igz.performance.database.DatabaseDAO)
	 */
	public void setDao(DatabaseDAO dao) {
		this.dao = dao;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Consumer#run()
	 */
	public void run() {

		Channel channel = null;
		try {
			
			connection = getConnection();
			channel = connection.createChannel();

			channel.queueDeclare(queueName, QUEUE_CONFIG_DURABLE, QUEUE_CONFIG_EXCLUSIVE, QUEUE_CONFIG_AUTODELETE, null);
			channel.basicQos(PREFETCH_COUNT);

			if (debug) {
				System.out.println(" [" + name + "] Consumer started");
			}

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, false, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());

				// Proccess message
				if (debug) {
					System.out.println(" [" + name + "] Received '" + StringUtils.abbreviate(message, 100) + "'");
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
				System.out.println(String.format("Queue [%s] deleted. Consumer [%s] cancelled.",queueName,name));
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
