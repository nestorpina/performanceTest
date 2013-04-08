package com.igz.performance.queue;

import com.igz.performance.queue.interfaces.Queue;

public abstract class AbstractQueue implements Queue {
	
	public enum OperationType {
		INSERT, SELECT
	}

	protected static final String HOST = "localhost";
	protected static final String SEPARATOR = "|";
	
	protected String queueName;
	protected boolean debug = false;

	public AbstractQueue(String queueName) {
		this.queueName = queueName;
	}
	
	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
}
