package loader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import loader.domain.Title;

public class MovieQueue {
	private BlockingQueue<Title> blockingQueue = new LinkedBlockingDeque<>(20);
	private boolean done = false;
	
	public void put(Title t) {
		try {
			blockingQueue.put(t);
		} catch (InterruptedException e) {
			//to do - log to error.log
			e.printStackTrace();
		}
	}
	
	public Title get() {
		try {
			return blockingQueue.take();
		} catch (InterruptedException e) {
			//to do - log to error.log
			e.printStackTrace();
			return null;
		}
	}
	
	public void done() {
		this.done = true;
		this.put(new Title());
	}
	
	public boolean isDone() {
		return done == true;
	}
	
}
