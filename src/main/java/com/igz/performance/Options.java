package com.igz.performance;

import com.igz.performance.database.DatabaseFactory.DatabaseType;
import com.igz.performance.queue.QueueFactory.QueueType;

public class Options {

	private DatabaseType databaseType;
	private QueueType queueType;
	private int numEvents;
	private int numWorkers;
	
	private boolean debug;
	private boolean showHelp;
	
	public int getNumWorkers() {
		return numWorkers;
	}
	public void setNumWorkers(int numWorkers) {
		this.numWorkers = numWorkers;
	}
	public boolean showHelp() {
		return showHelp;
	}
	public void setShowHelp(boolean showHelp) {
		this.showHelp = showHelp;
	}
	public DatabaseType getDatabaseType() {
		return databaseType;
	}
	public void setDatabaseType(DatabaseType databaseType) {
		this.databaseType = databaseType;
	}
	public QueueType getQueueType() {
		return queueType;
	}
	public void setQueueType(QueueType queueType) {
		this.queueType = queueType;
	}
	public int getNumEvents() {
		return numEvents;
	}
	public void setNumEvents(int numEvents) {
		this.numEvents = numEvents;
	}
	public boolean debug() {
		return debug;
	}
	public void setDebug(boolean debug) {
		this.debug = debug;
	}
	
}
