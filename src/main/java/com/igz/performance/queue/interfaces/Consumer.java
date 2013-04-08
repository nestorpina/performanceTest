package com.igz.performance.queue.interfaces;

import com.igz.performance.database.DatabaseDAO;

public interface Consumer extends Queue, Runnable {

	public abstract DatabaseDAO getDao();

	public abstract void setDao(DatabaseDAO dao);

	public abstract void run();

}