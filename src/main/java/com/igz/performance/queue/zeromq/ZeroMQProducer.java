package com.igz.performance.queue.zeromq;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.igz.performance.queue.interfaces.Producer;

public class ZeroMQProducer extends ZeroMq implements Producer {

	private String name;
	private int numberOfRequests;
	private OperationType operation;
	private String json;
	private List<String> idsToSelect;
	private Socket publisher;

	public ZeroMQProducer(String name, String queueName, int numberOfRequests) {
		super(queueName);
		this.name = name;
		this.numberOfRequests = numberOfRequests;
	}


	
	public void setIdsToSelect(List<String> idsToSelect) {
		this.idsToSelect = idsToSelect;
	}

	public void setOperation(OperationType operation) {
		this.operation = operation;

	}

	public void setJson(String json) {
		this.json = json;
	}

	public List<String> call() {
		List<String> ids = new ArrayList<String>();
		
		//  Prepare our context and publisher
        ZMQ.Context context = ZMQ.context(1);

        publisher = context.socket(ZMQ.PUSH);
        publisher.bind("tcp://*:5556");

    	if (operation == OperationType.INSERT) {
			ids = sendInserts();
		} else if (operation == OperationType.SELECT) {
			ids = sendSelects();
		}

    	publisher.close ();
        context.term ();
        
        return ids;
	}
	

	private List<String> sendSelects()  {
		List<String> ids = new ArrayList<String>();
		for (Object id : idsToSelect) {
			String message = operation + SEPARATOR + id;
			publisher.send(message, 0);
			if (debug) {
				System.out.println(" [" + name + "] Sent '" + message + "'");
			}
		}
		return ids;
	}

	private List<String> sendInserts() {
		List<String> ids = new ArrayList<String>();
		for (int i = 0; i < numberOfRequests; i++) {
			String id = UUID.randomUUID().toString();
			ids.add(id);
			String message = operation + SEPARATOR + id + SEPARATOR + json;
			publisher.send(message, 0);
			if (debug) {
				System.out.println(" [" + name + "] Sent '" + message.substring(0, 80) + "'" + (message.length() > 100 ? "..." : ""));
			}
		}
		return ids;
	}

}
