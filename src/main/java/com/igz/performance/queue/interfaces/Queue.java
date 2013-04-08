package com.igz.performance.queue.interfaces;

public interface Queue {

	public abstract int getPendingMessages();

	public abstract void deleteQueue();

	public abstract boolean isDebug();

	public abstract void setDebug(boolean debug);

}