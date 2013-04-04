package com.igz.performance.database;

public interface DatabaseDAO {

	public abstract void init();

	public abstract String insert(String id, String json);

	public abstract int select(String id);

	public abstract void removeAll();
	
	public abstract int count();
	
	public abstract void close() throws Throwable;

}