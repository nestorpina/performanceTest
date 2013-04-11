package com.igz.performance.queue;

import com.igz.performance.queue.amazonsqs.AmazonSQS;
import com.igz.performance.queue.interfaces.Queue;
import com.igz.performance.queue.none.DirectProducer;
import com.igz.performance.queue.rabbitmq.RabbitMQ;
import com.igz.performance.queue.zeromq.ZeroMq;

public class QueueFactory {
	
	public enum QueueType {
		NONE, RABBITMQ, ZEROMQ, AMAZONSQS
	}

	public static Queue createDatabase(QueueType type, String queueName) {
		switch (type) {
		case NONE:
			return new DirectProducer();
		case RABBITMQ:
			return new RabbitMQ(queueName);
		case ZEROMQ:
			return new ZeroMq(queueName);
		case AMAZONSQS:
			return new AmazonSQS(queueName);
		default:
			return new ZeroMq(queueName);
		}
		
		
	}
}
