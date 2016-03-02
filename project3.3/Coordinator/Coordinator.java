import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	// This integer variable tells you what region you are in
	// 1 for US-E, 2 for US-W, 3 for Singapore
	private static int region = KeyValueLib.region;

	// Default mode: Strongly consistent
	// Options: causal, eventual, strong
	private static String consistencyType = "strong";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances. Be sure to match the regions with their DNS!
	 * Do the same for the 3 Coordinators as well.
	 */
	private static final String dataCenterUSE = "ec2-52-23-179-136.compute-1.amazonaws.com";
	private static final String dataCenterUSW = "ec2-52-91-222-146.compute-1.amazonaws.com";
	private static final String dataCenterSING = "ec2-54-172-176-137.compute-1.amazonaws.com";

	private static final String coordinatorUSE = "ec2-54-173-28-33.compute-1.amazonaws.com";
	private static final String coordinatorUSW = "ec2-54-175-53-60.compute-1.amazonaws.com";
	private static final String coordinatorSING = "ec2-52-23-151-203.compute-1.amazonaws.com";

	private static String datastoreDNS;

	private static final int PRIME = 31;

	private ConcurrentHashMap<String, ReentrantLock> keyLock = null;
	private ConcurrentHashMap<String, PriorityBlockingQueue<queryEntry>> keyQueue = null;

	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenterUSE, 1);
		KeyValueLib.dataCenters.put(dataCenterUSW, 2);
		KeyValueLib.dataCenters.put(dataCenterSING, 3);
		KeyValueLib.coordinators.put(coordinatorUSE, 1);
		KeyValueLib.coordinators.put(coordinatorUSW, 2);
		KeyValueLib.coordinators.put(coordinatorSING, 3);

		keyLock = new ConcurrentHashMap<String, ReentrantLock>();
		keyQueue = new ConcurrentHashMap<String, PriorityBlockingQueue<queryEntry>>();

		if(region == 1) datastoreDNS = dataCenterUSE;
		else if(region == 2) datastoreDNS = dataCenterUSW;
		else datastoreDNS = dataCenterSING;

		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				if(consistencyType.equals("eventual")) {
					handleEventual(req);
				}
				else if(consistencyType.equals("strong")) {
					handleStrong(req);
				}
				else {
					handleCausal(req);
				}
			}

			//handle put request in strong consistency
			public void handleStrong(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				final String forwarded = map.get("forward");
				final String forwardedRegion = map.get("region");

				final int tartgetRegion = hashMethod(key);

				if(forwarded == null && tartgetRegion != region) {
					//if not primary coordinator, forward to the primary
					forward(key, value, timestamp, tartgetRegion);
				}
				else { //else, add to the PQ and start thread
					//change the time stamp if it is from another coordinator
					//new time stamp will be used for ordering in the queue
					Long newtimestamp = null;
					if(forwarded != null && forwarded.equals("true")) {
						newtimestamp = Skews.handleSkew(timestamp, 
								Integer.parseInt(forwardedRegion));
					}
					else {
						newtimestamp = timestamp;
					}
					final queryEntry query = new queryEntry(key, value, newtimestamp);
					PriorityBlockingQueue<queryEntry> pbq = getQueueFromMap(key);
					pbq.add(query);

					Thread t = new Thread(new Runnable() {
						public void run() {
							putStrong(key, value, query.time, query);
						}
					});
					t.start();
				}
				req.response().end(); // Do not remove this
			}

			//handle put request in causal consistency
			public void handleCausal(final HttpServerRequest req) {

				Thread t = new Thread(new Runnable() {

					public void run() {
						MultiMap map = req.params();
						final String key = map.get("key");
						final String value = map.get("value");
						final Long timestamp = Long.parseLong(map.get("timestamp"));
						final String forwarded = map.get("forward");
						final String forwardedRegion = map.get("region");

						final int tartgetRegion = hashMethod(key);

						if(forwarded == null && tartgetRegion != region) {
							//if not primary coordinator, forward to the primary
							forward(key, value, timestamp, tartgetRegion);
						}
						else { //else, add to the PQ and start thread
							//change the time stamp if it is from another coordinator
							//new time stamp will be used for ordering in the queue
							Long newtimestamp = null;
							if(forwarded != null && forwarded.equals("true")) {
								newtimestamp = Skews.handleSkew(timestamp, 
										Integer.parseInt(forwardedRegion));
							}
							else {
								newtimestamp = timestamp;
							}
							final queryEntry query = new queryEntry(key, value, newtimestamp);
							PriorityBlockingQueue<queryEntry> pbq = getQueueFromMap(key);
							pbq.add(query);
							putCausal(key, value, query.time, query);
						}
					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}

			//handle put request in eventual consistency
			public void handleEventual(final HttpServerRequest req) {
				Thread t = new Thread(new Runnable() {
					public void run() {
						MultiMap map = req.params();
						final String key = map.get("key");
						final String value = map.get("value");
						final Long timestamp = Long.parseLong(map.get("timestamp"));
						final String forwarded = map.get("forward");
						final String forwardedRegion = map.get("region");
						final int tartgetRegion = hashMethod(key);

						if(forwarded == null && tartgetRegion != region) {
							//if not primary coordinator, forward to the primary
							forward(key, value, timestamp, tartgetRegion);
						}
						else {
							putEventual(key, value, timestamp);
						}
					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}
		});

				routeMatcher.get("/get", new Handler<HttpServerRequest>() {
					@Override
					public void handle(final HttpServerRequest req) {
						if(consistencyType.equals("eventual")) {
							handleNoBlock(req);
						}
						else if(consistencyType.equals("strong")) {
							handleStrong(req);
						}
						else {
							handleNoBlock(req);
						}
					}

					//handle get request in strong consistency
					public void handleStrong(final HttpServerRequest req) {
						MultiMap map = req.params();
						final String key = map.get("key");
						final Long timestamp = Long.parseLong(map.get("timestamp"));
						final queryEntry query = new queryEntry(key, null, timestamp);
						PriorityBlockingQueue<queryEntry> pbq = getQueueFromMap(key);
						pbq.add(query);

						Thread t = new Thread(new Runnable() {
							public void run() {
								req.response().end(getStrong(key, timestamp, query));
							}
						});
						t.start();
					}

					//handle get request in causal and eventual consistency
					public void handleNoBlock(final HttpServerRequest req) {
						Thread t = new Thread(new Runnable() {
							public void run() {
								MultiMap map = req.params();
								final String key = map.get("key");
								final Long timestamp = Long.parseLong(map.get("timestamp"));
								req.response().end(getNoBlock(key, timestamp));
							}
						});
						t.start();
					}
				});

				/* This endpoint is used by the grader to change the consistency level */
				routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
					@Override
					public void handle(final HttpServerRequest req) {
						MultiMap map = req.params();
						consistencyType = map.get("consistency");
						req.response().end();
					}
				});
				/* BONUS HANDLERS BELOW */
				routeMatcher.get("/forwardcount", new Handler<HttpServerRequest>() {
					@Override
					public void handle(final HttpServerRequest req) {
						req.response().end(KeyValueLib.COUNT());
					}
				});

				routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
					@Override
					public void handle(final HttpServerRequest req) {
						System.out.println("reseted, next test....");
						KeyValueLib.RESET();
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

	/* do put request for strong consistency */
	private void putStrong(String key, String value, Long timestamp,
			queryEntry query) {
		// if this is forwarded from another coordinator, means self is the
		// primary
		ReentrantLock thisLock = getLockFromMap(key);
		PriorityBlockingQueue<queryEntry> thisQueue = keyQueue.get(key);
		// notify DC in advance the put request so that it can schedule
		try {
			KeyValueLib.AHEAD(key, Long.toString(timestamp));
		} catch (IOException e) {
			e.printStackTrace();
		}
		thisLock.lock();
		Condition thisCondition = thisLock.newCondition();

		while (thisQueue.peek() != query) { // wait for queue front is self
			try {
				thisCondition.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try { // do put request, will block
			put(key, value, timestamp);
			thisQueue.poll();
			thisCondition.signalAll();
		} finally {
			thisLock.unlock();
		}
	}

	/* do get request for strong consistency */
	private String getStrong(String key, Long timestamp, queryEntry query) {
		String response = "0";
		ReentrantLock thisLock = getLockFromMap(key);
		PriorityBlockingQueue<queryEntry> thisQueue = keyQueue.get(key);
		thisLock.lock();
		Condition thisCondition = thisLock.newCondition();
		while (thisQueue.peek() != query) { // wait for queue front is self
			try {
				thisCondition.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try { // do get request, will block
			response = get(key, timestamp);
			thisQueue.poll();
			thisCondition.signalAll();
		} finally {
			thisLock.unlock();
		}

		return response;
	}

	/*
	 * do put method with causal consistency has different locking on different
	 * DCs, PUT will not block
	 */
	private void putCausal(String key, String value, Long timestamp,
			queryEntry query) {
		ReentrantLock thisLock = getLockFromMap(key);
		PriorityBlockingQueue<queryEntry> thisQueue = keyQueue.get(key);
		thisLock.lock();
		Condition thisCondition = thisLock.newCondition();

		while (thisQueue.peek() != query) {
			try {
				thisCondition.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		final String finalkey = key;
		final String finalvalue = value;
		final Long finaltimestamp = timestamp;

		// create three threads so that PUT will not block
		try {
			System.out.println("PUT: " + key + ": " + value + ": " + timestamp);
			Thread t1 = new Thread(new Runnable() {
				public void run() {
					try {
						KeyValueLib.PUT(dataCenterUSE, finalkey, finalvalue,
								Long.toString(finaltimestamp), consistencyType);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
			Thread t2 = new Thread(new Runnable() {
				public void run() {
					try {
						KeyValueLib.PUT(dataCenterUSW, finalkey, finalvalue,
								Long.toString(finaltimestamp), consistencyType);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
			Thread t3 = new Thread(new Runnable() {
				public void run() {
					try {
						KeyValueLib.PUT(dataCenterSING, finalkey, finalvalue,
								Long.toString(finaltimestamp), consistencyType);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
			t1.start();
			t2.start();
			t3.start();
			thisQueue.poll();
			thisCondition.signalAll();
		} finally {
			thisLock.unlock();
		}
	}

	/* do put method without any locking */
	private void putEventual(String key, String value, Long timestamp) {
		final String finalkey = key;
		final String finalvalue = value;
		final Long finaltimestamp = timestamp;
		System.out.println("PUT: " + key + ": " + value + ": " + timestamp);
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSE, finalkey, finalvalue,
							Long.toString(finaltimestamp), consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSW, finalkey, finalvalue,
							Long.toString(finaltimestamp), consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		Thread t3 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterSING, finalkey, finalvalue,
							Long.toString(finaltimestamp), consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t1.start();
		t2.start();
		t3.start();
	}

	/* do get method without any blocking and locking */
	private String getNoBlock(String key, Long timestamp) {
		String response = "0";
		response = get(key, timestamp);
		return response;
	}

	/* help methods for PUT, FORWARD and GET */
	/*
	 * put will have to identify the primary coordinator associated with this
	 * key
	 */
	private void put(String key, String value, Long timestamp) {
		try {
			System.out.println("PUT: " + key + ": " + value + ": " + timestamp);
			KeyValueLib.PUT(dataCenterUSE, key, value,
					Long.toString(timestamp), consistencyType);
			KeyValueLib.PUT(dataCenterUSW, key, value,
					Long.toString(timestamp), consistencyType);
			KeyValueLib.PUT(dataCenterSING, key, value,
					Long.toString(timestamp), consistencyType);
			KeyValueLib.COMPLETE(key, Long.toString(timestamp));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void forward(String key, String value, Long timestamp,
			int tartgetRegion) {
		try {
			switch (tartgetRegion) {
			case Constants.US_EAST:
				KeyValueLib.FORWARD(coordinatorUSE, key, value,
						Long.toString(timestamp));
				break;
			case Constants.US_WEST:
				KeyValueLib.FORWARD(coordinatorUSW, key, value,
						Long.toString(timestamp));
				break;
			case Constants.SINGAPORE:
				KeyValueLib.FORWARD(coordinatorSING, key, value,
						Long.toString(timestamp));
				break;
			default:
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String get(String key, Long timestamp) {
		try {
			System.out.println("GET: " + key + ": " + timestamp);
			return KeyValueLib.GET(datastoreDNS, key, Long.toString(timestamp),
					consistencyType);
		} catch (IOException e) {
			e.printStackTrace();
			return "0";
		}
	}

	/*
	 * for given key, return 1-3 if it is a-c, return DC index based on hashcode
	 * otherwise
	 */
	private int hashMethod(String key) {
		if (key.equals("a"))
			return 1;
		if (key.equals("b"))
			return 2;
		if (key.equals("c"))
			return 3;
		int hashCode = 0;
		for (int i = 0; i < key.length(); i++) {
			hashCode = hashCode * PRIME + (int) key.charAt(i);
		}
		// take care of overflow issue
		if (hashCode < 0)
			hashCode = -hashCode;
		// map the hash code to DC index by % 3 + 1
		return hashCode % 3 + 1;
	}

	/* help method for get lock from a key */
	private ReentrantLock getLockFromMap(String key) {
		if (!keyLock.containsKey(key)) {
			ReentrantLock thisLock = new ReentrantLock();
			keyLock.put(key, thisLock);
			return thisLock;
		} else {
			return keyLock.get(key);
		}
	}

	/* help method for get queue from a key */
	private PriorityBlockingQueue<queryEntry> getQueueFromMap(String key) {
		if (!keyQueue.containsKey(key)) {
			PriorityBlockingQueue<queryEntry> thisQueue = new PriorityBlockingQueue<queryEntry>(
					1, new compareTime());
			keyQueue.put(key, thisQueue);
			return thisQueue;
		} else {
			return keyQueue.get(key);
		}
	}

	/*
	 * nested class for storing method, key, value and timestamp in priority
	 * queue
	 */
	private class queryEntry {
		public final String k;
		public final String v;
		public final Long time;

		public queryEntry(String k, String v, Long time) {
			this.k = k;
			this.v = v;
			this.time = time;
		}
	}

	/* comparator for priority queue, earlier time is smaller */
	private class compareTime implements Comparator<queryEntry> {
		@Override
		public int compare(queryEntry first, queryEntry second) {
			if (first.time - second.time > 0) {
				return 1;
			} else if (first.time - second.time < 0) {
				return -1;
			} else
				return 0;
		}
	}
}
