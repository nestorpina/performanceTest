package com.igz.performance.queue.none;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.queue.AbstractQueue.OperationType;
import com.igz.performance.queue.interfaces.Consumer;
import com.igz.performance.queue.interfaces.Producer;

public class DirectProducer implements Producer {

	private String name;
	private int numberOfRequests;
	private OperationType operation;
	private String json;
	private List<String> idsToSelect;
	private boolean debug;
	private DatabaseDAO dao;

	public DirectProducer() {};
	
	public DirectProducer(String name, String queueName, int numberOfRequests) {
		this.name = name;
		this.numberOfRequests = numberOfRequests;
	}
	

	public Consumer createConsumer(String name, DatabaseDAO databaseDAO) {
		throw new UnsupportedOperationException("method createConsumer not implemented");
	}

	public Producer createProducer(String name, int numberOfRequests) {
		return new DirectProducer(name, null, numberOfRequests);
	}

	public List<String> call() {
		List<String> ids = new ArrayList<String>();
		
    	if (operation == OperationType.INSERT) {
			ids = sendInserts();
		} else if (operation == OperationType.SELECT) {
			ids = sendSelects();
		}

        return ids;
	}
	

	private List<String> sendSelects()  {
		List<String> ids = new ArrayList<String>();
		for (String id : idsToSelect) {
			if (debug) {
				System.out.println(" [" + name + "] Execute select with id: '" + id + "'");
			}
			dao.select(id);
		}
		return ids;
	}

	private List<String> sendInserts() {
		List<String> ids = new ArrayList<String>();
		for (int i = 0; i < numberOfRequests; i++) {
			String id = UUID.randomUUID().toString();
			ids.add(id);
			if (debug) {
				System.out.println(String.format(" [%s] Execute insert with id: '%s' content: '%s'", name, id,
						StringUtils.abbreviate(json, 100)));
			}
			dao.insert(id, json);
		}
		return ids;
	}


	public int getPendingMessages() {
		return 0;
	}

	public void deleteQueue() { }

	
	public void setIdsToSelect(List<String> idsToSelect) {
		this.idsToSelect = idsToSelect;
	}

	public void setOperation(OperationType operation) {
		this.operation = operation;

	}

	public void setJson(String json) {
		this.json = json;
	}
	
	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;		
	}

	public DatabaseDAO getDao() {
		return dao;
	}

	public void setDao(DatabaseDAO dao) {
		this.dao = dao;
	}

}
