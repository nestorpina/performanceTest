package com.igz.performance.database;

public interface DatabaseDAO {

	public abstract void init();

	public abstract Object insert(Object id, String json);

	public abstract int select(Object id);

	public abstract void removeAll();
	
	public abstract int count();

}