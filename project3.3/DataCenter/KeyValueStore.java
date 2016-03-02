import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;


public class KeyValueStore extends Verticle {
	private HashMap<String, ArrayList<StoreValue>> store = null;
	
	private ConcurrentHashMap<String, ReentrantLock> keyLock = null;
	private ConcurrentHashMap<String, PriorityBlockingQueue<Long>> keytimestamp = null;
	
	public KeyValueStore() {
		store = new HashMap<String, ArrayList<StoreValue>>();
	}

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();

		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);
		keyLock = new ConcurrentHashMap<String, ReentrantLock>();

		keytimestamp = new ConcurrentHashMap<String, PriorityBlockingQueue<Long>>();
		
		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {

				/* TODO: Add code to store the object here. You may need to adjust the timestamp */
				Thread t = new Thread(new Runnable() {
					public void run() {
						MultiMap map = req.params();
						final String key = map.get("key");
						String value = map.get("value");
						final String consistency = map.get("consistency");
						Integer region = Integer.parseInt(map.get("region"));
						Long timestamp = Long.parseLong(map.get("timestamp"));
						timestamp = Skews.handleSkew(timestamp, region);
						/* TODO: You will need to adjust the timestamp here for some consistency levels */
						
						final Long finaltimestamp = timestamp;
						final StoreValue sv = new StoreValue(timestamp, value);
						
						ArrayList<StoreValue> thisKeyList = keyValueStore.store.get(key);

						thisKeyList.add(sv);
						
						//if not eventual, sort the list to keep time order
						if(!consistency.equals("eventual"))
							Collections.sort(thisKeyList, new compareTime());
						
						System.out.println("PUT with: " + key + ": " + finaltimestamp);

						String response = "stored";
						req.response().putHeader("Content-Type", "text/plain");
						req.response().putHeader("Content-Length",
								String.valueOf(response.length()));
						req.response().end(response);
						req.response().close();
					}
				});
				t.start();
			}
		});
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				/* TODO: Add code here to get the list of StoreValue associated with the key 
				 * Remember that you may need to implement some locking on certain consistency levels */
				Thread t = new Thread(new Runnable() {
					public void run() {
						MultiMap map = req.params();
						final String key = map.get("key");
						final String consistency = map.get("consistency");
						final Long timestamp = Long.parseLong(map.get("timestamp"));
						
						ArrayList<StoreValue> values = null;
						System.out.println(consistency);
						//do get based on consistency time
						if(consistency.equals("strong")) {
							values = getStrong(key, timestamp, keyValueStore);
						}
						else if(consistency.equals("causal")) {
							values = getNotStrong(key, timestamp, keyValueStore);
						}
						else {
							values = getNotStrong(key, timestamp, keyValueStore);
						}
						
						/* Do NOT change the format the response. It will return a string of
						 * values separated by spaces */
						String response = "";
						if (values != null) {
							for (StoreValue val : values) {
								response = response + val.getValue() + " ";
							}
						}
						req.response().putHeader("Content-Type", "text/plain");
						if (response != null)
							req.response().putHeader("Content-Length",
									String.valueOf(response.length()));
						req.response().end(response);
						req.response().close();
					}
				});
				t.start();

			}
		});
		// Handler for when the AHEAD is called
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				System.out.println("AHEAD: " + key + ": " + timestamp);

				Thread t = new Thread(new Runnable() {
					public void run() {
						keytimestamp.get(key).add(timestamp);
					}
				});
				t.start();
				
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the COMPLETE is called
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));	
				System.out.println("COMPLETE: " + key + ": " + timestamp);
				
				keytimestamp.get(key).poll();
				
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Clears this stored keys. Do not change this
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				System.out.println("reseted, next test....");
				keyValueStore.store.clear();
				
				/*try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				
				keyValueStore.store.put("a", new ArrayList<StoreValue>());
				keyValueStore.store.put("b", new ArrayList<StoreValue>());
				keyValueStore.store.put("c", new ArrayList<StoreValue>());
				
				keytimestamp.put("a", new PriorityBlockingQueue<Long>());
				keytimestamp.put("b", new PriorityBlockingQueue<Long>());
				keytimestamp.put("c", new PriorityBlockingQueue<Long>());

				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
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
	
	/* get value from map for strong consistency */
	private ArrayList<StoreValue> getStrong(String key, Long timestamp, KeyValueStore keyValueStore) {
		ArrayList<StoreValue> values = null;
		System.out.println(keytimestamp.get(key).peek());
		while(true) { 
			//wait for complete signal, block if not complete or queue
			//front has smaller timestamp 
			Long frontTimestamp = keytimestamp.get(key).peek();
			if(frontTimestamp == null)
				break;
			if(frontTimestamp > timestamp)
				break;
		}
		System.out.println("GET: " + key + ": " + timestamp);
		if(keyValueStore.store.containsKey(key))
			values = keyValueStore.store.get(key);
		return values;
	}
	
	/* get value from map for causal and eventual consistency */
	private ArrayList<StoreValue> getNotStrong(String key, Long timestamp, KeyValueStore keyValueStore) {
		ArrayList<StoreValue> values = null;

		System.out.println("GET with: " + key + ": " + timestamp);
		if(keyValueStore.store.containsKey(key))
			values = keyValueStore.store.get(key);
		return values;
	}
	
	/* help method for get lock from a key */
	private ReentrantLock getLockFromMap(String key) {
		if(!keyLock.containsKey(key)) {
			ReentrantLock thisLock = 
					new ReentrantLock();
			keyLock.put(key, thisLock);
			return thisLock;
		}
		else {
			return keyLock.get(key);
		}
	}
	
	/* comparator for priority queue, earlier time is smaller */
	private class compareTime implements Comparator<StoreValue>{
		@Override
		public int compare(StoreValue first, StoreValue second) {
			Long timediff = first.getTimestamp() - second.getTimestamp();
			if(timediff == 0) return 0;
			else if(timediff > 0) return 1;
			else return -1;
		}
	}
}
