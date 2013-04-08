package com.igz.performance.queue.interfaces;

import java.util.List;
import java.util.concurrent.Callable;

import com.igz.performance.queue.AbstractQueue.OperationType;

public interface Producer extends Queue, Callable<List<String>>{

	public abstract void setIdsToSelect(List<String> idsToSelect);

	public abstract void setOperation(OperationType operation);

	public abstract void setJson(String json);

	public abstract List<String> call();

}