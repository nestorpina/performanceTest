package com.igz.performance.queue;

import com.igz.performance.queue.interfaces.Queue;
import com.igz.performance.queue.rabbitmq.RabbitMQ;
import com.igz.performance.queue.zeromq.ZeroMq;

public class QueueFactory {
	
	public enum QueueType {
		NONE, RABBITMQ, ZEROMQ
	}

	public static Queue createDatabase(QueueType type, String queueName) {
		switch (type) {
		case NONE:
			return new ZeroMq(queueName);
		case RABBITMQ:
			return new RabbitMQ(queueName);
		case ZEROMQ:
			return new ZeroMq(queueName);
		default:
			return new ZeroMq(queueName);
		}
		
		
	}
}
