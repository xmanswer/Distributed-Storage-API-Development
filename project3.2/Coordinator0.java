import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.sql.Timestamp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator0 extends Verticle {

	//Default mode: replication. Possible string values are "replication" and "sharding"
	private static String storageType = "replication";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-52-91-197-49.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-52-91-95-131.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-52-91-111-237.compute-1.amazonaws.com";
	//concurrent hashmap for storing key and asscociated priority queue of queries
	private ConcurrentHashMap<String, PriorityBlockingQueue<queryEntry>> keyQueueMap;
	//concurrent hashmap for storing key and asscociated lock
	private ConcurrentHashMap<String, ReentrantLock> keyLockMap;

	@Override
	public void start() {
		//DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				//You may use the following timestamp for ordering requests
				final Timestamp timestamp = new Timestamp(System.currentTimeMillis() 
						+ TimeZone.getTimeZone("EST").getRawOffset());
				
				//create query object
				final queryEntry putQuery = new queryEntry("put", key, value, timestamp);
				

				
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for PUT operation here.
						//Each PUT operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.\
						concurrentOperation(putQuery);
					}
				});
				t.start();
				req.response().end(); //Do not remove this
			}
		});
		
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				//You may use the following timestamp for ordering requests
				final Timestamp timestamp = new Timestamp(System.currentTimeMillis() 
						+ TimeZone.getTimeZone("EST").getRawOffset());
				
				//create query object
				final queryEntry getQuery = new queryEntry("get", key, loc, timestamp);


				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for GET operation here.
						//Each GET operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.
						req.response().end(concurrentOperation(getQuery)); //Default response = 0
					}
				});
				t.start();
			}
		});

		routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				storageType = map.get("storage");
				//This endpoint will be used by the auto-grader to set the 
				//consistency type that your key-value store has to support.
				//You can initialize/re-initialize the required data structures here
				keyQueueMap = new 
						ConcurrentHashMap<String, PriorityBlockingQueue<queryEntry>>();
				keyLockMap = new ConcurrentHashMap<String, ReentrantLock>();
				req.response().end();
			}
		});

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
	
	/* concurrent operation based on query, return null if it is put, 
	 * return get result if it is get */
	public String concurrentOperation(queryEntry query) {
		
		String resultStr = null;
		String key = query.k;
		
		//check if key exists in the two hash maps, if not, create new one
		if(!keyLockMap.containsKey(key)) {
			keyLockMap.put(key, new ReentrantLock());
		}
		if(!keyQueueMap.containsKey(key)) {
			keyQueueMap.put(key, new PriorityBlockingQueue<queryEntry>(1, new compareTime()));
		}
		
		//get associated priority queue and add this new query to it 
		PriorityBlockingQueue<queryEntry> thisQueue = keyQueueMap.get(key);
		thisQueue.add(query);
		
		ReentrantLock thisLock = keyLockMap.get(key);
		
		thisLock.lock(); 
		Condition thisCondition = thisLock.newCondition();
		
		//wait until the given query is in the front of PQ 
		while(thisQueue.peek() != query) {
			try {
				thisCondition.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try { //based on method string, choose to put or get data
			if(query.method.equals("put")) {
				concurrentPut(query);
			}
			else {
				resultStr = concurrentGet(query);
			}
			/* remove the query from the queue, 
			 * if no more query in queue, remove it from
			 * hashmap to prevent memory leak */
			thisQueue.poll();
			if(thisQueue.size() == 0) {
				keyLockMap.remove(key);
			}
			
			//tell other threads that I'm done
			thisCondition.signalAll();
		} finally {
			thisLock.unlock();
		}
		return resultStr;
	}
	
	private void concurrentPut(queryEntry putQuery) {
		try {
			KeyValueLib.PUT(dataCenter1, putQuery.k, putQuery.v);
			KeyValueLib.PUT(dataCenter2, putQuery.k, putQuery.v);
			KeyValueLib.PUT(dataCenter3, putQuery.k, putQuery.v);
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private String concurrentGet(queryEntry getQuery) {
		String resultStr = "0";
		String loc = getQuery.v;
		try {
			if(loc.equals("1"))
				resultStr = KeyValueLib.GET(dataCenter1, getQuery.k);
			else if(loc.equals("2"))
				resultStr = KeyValueLib.GET(dataCenter2, getQuery.k);
			else if(loc.equals("3"))
				resultStr = KeyValueLib.GET(dataCenter3, getQuery.k);
			else
				resultStr = "0";
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return resultStr;
	}
	
	/* nested class for storing method, key, value and timestamp in priority queue */
	private class queryEntry {
		public final String method;
		public final String k;
		public final String v;
		public final Timestamp time;
		
		public queryEntry(String method, String k, String v, Timestamp time) {
			this.method = method;
			this.k = k;
			this.v = v;
			this.time = time;
		}
	}
	
	/* comparator for priority queue, earlier time is smaller */
	private class compareTime implements Comparator<queryEntry>{
		@Override
		public int compare(queryEntry first, queryEntry second) {
			if(first.time.after(second.time)) {
				return 1;
			}
			else if(first.time.before(second.time)) {
				return -1;
			}
			else return 0;
		}
	}
}
