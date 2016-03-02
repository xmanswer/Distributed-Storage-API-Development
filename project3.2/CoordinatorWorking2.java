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

public class CoordinatorWorking2 extends Verticle {

	//Default mode: replication. Possible string values are "replication" and "sharding"
	private static String storageType = "replication";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-54-85-222-242.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-52-91-90-103.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-52-91-90-5.compute-1.amazonaws.com";
	//concurrent hashmap for storing key and asscociated priority queue of queries
	private ConcurrentHashMap<String, PriorityBlockingQueue<queryEntry>> keyQueueMap;
	//concurrent hashmap for storing key and asscociated lock
	private ConcurrentHashMap<String, ReentrantLock> keyLockMap;
	//concurrent hashmap for storing data store number and asscociated priority queue of queries
	private ConcurrentHashMap<Integer, PriorityBlockingQueue<queryEntry>> dsMap;
	//concurrent hashmap for storing data store number and asscociated lock
	private ConcurrentHashMap<Integer, ReentrantLock> dsLockMap;
		
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
				
				//check if key exists in the two hash maps, if not, create new one
				if(!keyLockMap.containsKey(key)) {
					keyLockMap.put(key, new ReentrantLock());
				}
				if(!keyQueueMap.containsKey(key)) {
					keyQueueMap.put(key, new PriorityBlockingQueue<queryEntry>(1, new compareTime()));
				}
				PriorityBlockingQueue<queryEntry> thisQueue = keyQueueMap.get(key);
				thisQueue.add(putQuery);
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for PUT operation here.
						//Each PUT operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.\
						if(storageType.equals("replication"))
							replicationOperation(putQuery);
						else
							shardingConcurrentPut(putQuery);
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
				
				//check if key exists in the two hash maps, if not, create new one
				if(!keyLockMap.containsKey(key)) {
					keyLockMap.put(key, new ReentrantLock());
				}
				if(!keyQueueMap.containsKey(key)) {
					keyQueueMap.put(key, new PriorityBlockingQueue<queryEntry>(1, new compareTime()));
				}
				PriorityBlockingQueue<queryEntry> thisQueue = keyQueueMap.get(key);
				thisQueue.add(getQuery);
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for GET operation here.
						//Each GET operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.
						if(storageType.equals("replication"))
							req.response().end(replicationOperation(getQuery)); //Default response = 0
						else
							req.response().end(shardingConcurrentGet(getQuery));
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
				//if sharing, initialize dsMap
				if(!storageType.equals("storage")) {
					dsMap = new ConcurrentHashMap<Integer, PriorityBlockingQueue<queryEntry>>();
					dsMap.put(1, new PriorityBlockingQueue<queryEntry>(1, new compareTime()));
					dsMap.put(2, new PriorityBlockingQueue<queryEntry>(1, new compareTime()));
					dsMap.put(3, new PriorityBlockingQueue<queryEntry>(1, new compareTime()));
					dsLockMap = new ConcurrentHashMap<Integer, ReentrantLock>();
					dsLockMap.put(1, new ReentrantLock());
					dsLockMap.put(2, new ReentrantLock());
					dsLockMap.put(3, new ReentrantLock());
				}
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
	public String replicationOperation(queryEntry query) {
		
		String resultStr = null;
		String key = query.k;
		
		//get associated priority queue and add this new query to it 
		PriorityBlockingQueue<queryEntry> thisQueue = keyQueueMap.get(key);
		//thisQueue.add(query);
		
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
				replicationPut(query);
			}
			else {
				resultStr = replicationGet(query);
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
	
	private void shardingConcurrentPut(queryEntry query) {
		
		String key = query.k;
		int hashResult = hashMethod(key);
		
		PriorityBlockingQueue<queryEntry> thisDSQueue = dsMap.get(hashResult);
		PriorityBlockingQueue<queryEntry> thisKeyQueue = keyQueueMap.get(key);
		
		thisDSQueue.add(query);
				
		ReentrantLock keyLock = keyLockMap.get(key);
		ReentrantLock dsLock = dsLockMap.get(hashResult);
		
		keyLock.lock(); 
		dsLock.lock();
		
		Condition keyCondition = keyLock.newCondition();
		Condition dsCondition = dsLock.newCondition();
		
		//wait until the given query is in the front of PQ 
		while(thisDSQueue.peek() != query) {
			try {
				dsCondition.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try { 
			shardingPut(hashResult, query);
			//System.out.println(query.method + " " + query.k + " " + query.v);
			/* remove the query from the queue, 
			 * if no more query in queue, remove it from
			 * hashmap to prevent memory leak */
			thisDSQueue.poll();
			thisKeyQueue.poll();
			
			if(thisKeyQueue.size() == 0) {
				keyLockMap.remove(key);
			}
			
			//tell other threads that I'm done
			dsCondition.signalAll();
			keyCondition.signalAll();
		} finally {
			dsLock.unlock();
			keyLock.unlock();
		}
	}
	
	private String shardingConcurrentGet(queryEntry query) {

		String resultStr = "0";
		String key = query.k;
		int hashResult = hashMethod(key);
		
		PriorityBlockingQueue<queryEntry> thisKeyQueue = keyQueueMap.get(key);
				
		ReentrantLock keyLock = keyLockMap.get(key);
		
		keyLock.lock(); 
		
		Condition keyCondition = keyLock.newCondition();
		
		//wait until the given query is in the front of PQ 
		while(thisKeyQueue.peek() != query) {
			try {
				keyCondition.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try { 
			resultStr = shardingGet(hashResult, query);
			
			/* remove the query from the queue, 
			 * if no more query in queue, remove it from
			 * hashmap to prevent memory leak */
			thisKeyQueue.poll();
			//System.out.println(query.method + " " + query.k + " " + query.v);
			if(thisKeyQueue.size() == 0) {
				keyLockMap.remove(key);
			}
			
			//tell other threads that I'm done
			keyCondition.signalAll();
		} finally {
			keyLock.unlock();
		}
		return resultStr;
	}
	
	private int hashMethod(String key) {
		if(key.equals("a")) return 1;
		if(key.equals("b")) return 2;
		if(key.equals("c")) return 3;
		int hashCode = key.hashCode() % 3;
		if(hashCode < 0) hashCode = -hashCode;
		return  hashCode + 1;
	}
	
	/* put key value in all data stores, this is for replication */
	private void replicationPut(queryEntry putQuery) {
		try {
			KeyValueLib.PUT(dataCenter1, putQuery.k, putQuery.v);
			KeyValueLib.PUT(dataCenter2, putQuery.k, putQuery.v);
			KeyValueLib.PUT(dataCenter3, putQuery.k, putQuery.v);
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	/* put key value in one data store, this is for sharding */
	private void shardingPut(int hashResult, queryEntry putQuery) {
		//System.out.println(putQuery.method + " " + putQuery.k + " " + putQuery.v);
		try {
			if(hashResult == 1)
				KeyValueLib.PUT(dataCenter1, putQuery.k, putQuery.v);
			else if(hashResult == 2)
				KeyValueLib.PUT(dataCenter2, putQuery.k, putQuery.v);
			else
				KeyValueLib.PUT(dataCenter3, putQuery.k, putQuery.v);
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	/* get key value from specific data store, this is for replication */
	private String replicationGet(queryEntry getQuery) {
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
	
	/* get key value from specific data store, this is for sharding 
	 * if loc is not hash result, return 0, otherwise return data */
	private String shardingGet(int hashResult, queryEntry getQuery) {
		String resultStr = "0";
		String loc = getQuery.v;
		//System.out.println(getQuery.method + " " + getQuery.k + " " + getQuery.v);
		if(!loc.equals(Integer.toString(hashResult))) return "0";
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
