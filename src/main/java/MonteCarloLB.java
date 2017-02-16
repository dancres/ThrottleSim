import java.util.*;
import java.util.concurrent.*;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class MonteCarloLB {
	// Percentage of requests that fall in 100ms ranges starting at 0-100ms (long-tailed distribution so trimmed & not summing to 100%)
	//
	private static final double[] BUCKET_SIZE_PERCENTAGES = {12.62, 25.58, 9.53, 7.04, 6.15, 5.42, 4.58, 3.74,
			3.01, 2.44, 2.01, 1.70, 1.45, 1.25, 1.09, 0.97, 0.86, 0.77, 0.70, 0.64, 0.59, 0.65, 0.56, 0.50, 0.46,
			0.42, 0.38, 0.35, 0.32, 0.29, 0.26, 0.24, 0.22, 0.20, 0.19, 0.17, 0.16, 0.15, 0.13, 0.12, 0.11, 0.10,
			0.10, 0.09, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.04};
	
	// Number of cores to use for simulations
	//
	private static OptionSpec<Integer> NUM_CORES;

	// Number of cycles to run per throttle setting
	//
	private static OptionSpec<Integer> SIMS_PER_SETTING;

	// How long each simulated cycle should be
	//
	private static OptionSpec<Integer> RUN_TIME_IN_SECONDS;

	// How many buckets in the distribution to use
	//
	private static OptionSpec<Integer> MAX_CONTRIBUTING_BUCKET;

	// Requests per minute
	//
	private static OptionSpec<Integer> REQUESTS_PER_MINUTE;
	private static int REQUESTS_PER_SEC;

	// The initial setting for the per machine throttle
	//
	private static OptionSpec<Integer> THROTTLE_BASE;

	// Number of machines in the cluster
	//
	private static OptionSpec<Integer> TOTAL_SERVERS;

	private static int[] BUCKET_TIMES_MILLIS;
	private static int[] REQS_PER_BUCKET;

	private OptionSet _options;

	MonteCarloLB(OptionSet aSet) {
		_options = aSet;
	}

	private static OptionSet parseOptions(String[] anArgs) {
		OptionParser myOp = new OptionParser();

		NUM_CORES = myOp.accepts("c").withOptionalArg().ofType(Integer.class).defaultsTo(2);
		SIMS_PER_SETTING = myOp.accepts("s").withOptionalArg().ofType(Integer.class).defaultsTo(12);
		RUN_TIME_IN_SECONDS = myOp.accepts("t").withOptionalArg().ofType(Integer.class).defaultsTo(60);
		MAX_CONTRIBUTING_BUCKET = myOp.accepts("b").withOptionalArg().ofType(Integer.class).defaultsTo(BUCKET_SIZE_PERCENTAGES.length);
		REQUESTS_PER_MINUTE = myOp.accepts("r").withOptionalArg().ofType(Integer.class).defaultsTo(160000);
		THROTTLE_BASE = myOp.accepts("l").withOptionalArg().ofType(Integer.class).defaultsTo(25);
		TOTAL_SERVERS = myOp.accepts("h").withOptionalArg().ofType(Integer.class).defaultsTo(200);

		return myOp.parse(anArgs);
	}

	private static OptionSet init(String[] anArgs) {
		OptionSet myOptions = parseOptions(anArgs);
 		REQUESTS_PER_SEC = REQUESTS_PER_MINUTE.value(myOptions) / 60;

		BUCKET_TIMES_MILLIS = new int[MAX_CONTRIBUTING_BUCKET.value(myOptions) + 1];
		BUCKET_TIMES_MILLIS[0] = 0;

		System.out.print("Bucket ceilings: ");

		for (int i = 0; i < MAX_CONTRIBUTING_BUCKET.value(myOptions); i++) {
			BUCKET_TIMES_MILLIS[1 + i] = 100 * (1 + i);
		}

		for (int i = 1; i < BUCKET_TIMES_MILLIS.length; i++)
			System.out.print(BUCKET_TIMES_MILLIS[i] + " ms ");

		System.out.println();
		System.out.println();
		System.out.print("Request distribution: ");

		REQS_PER_BUCKET = new int[MAX_CONTRIBUTING_BUCKET.value(myOptions)];

		for (int i = 0; i < MAX_CONTRIBUTING_BUCKET.value(myOptions); i++) {
			int myReqs = (int) (REQUESTS_PER_SEC * BUCKET_SIZE_PERCENTAGES[i] / 100);
			REQS_PER_BUCKET[i] = (myReqs == 0) ? 1 : myReqs;

			System.out.print("(" + i + ") " + REQS_PER_BUCKET[i] + " requests ");
		}

		System.out.println();
		System.out.println();

		return myOptions;
	}

	public static void main(String[] anArgs) throws Exception {
		new MonteCarloLB(init(anArgs)).simulate();
	}

	private void simulate() throws Exception {
		System.out.println("Run-time (s): " + RUN_TIME_IN_SECONDS.value(_options) + " @ " + 
			REQUESTS_PER_MINUTE.value(_options) + " rpm (" + REQUESTS_PER_SEC + " rps)");
		System.out.println("Cores: " + NUM_CORES.value(_options));

		ThreadPoolExecutor myExecutor = new ThreadPoolExecutor(NUM_CORES.value(_options), NUM_CORES.value(_options), 30,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

		int myCurrentThrottle = THROTTLE_BASE.value(_options);
		
		while (true) {
			long myRequestsTotal = 0;
			long myBreachesTotal = 0;
			LinkedList<FutureTask<Simulator>> mySims = new LinkedList<>();

			System.out.println("Throttle limit: " + myCurrentThrottle + " requests per server of which there are: " + 
				TOTAL_SERVERS.value(_options));

			for (int i = 0; i < SIMS_PER_SETTING.value(_options); i++) {
				FutureTask<Simulator> myTask =
						new FutureTask<>(new Simulator(myCurrentThrottle, generateDurations(), TOTAL_SERVERS.value(_options)));
				mySims.add(myTask);
				myExecutor.execute(myTask);
			}

			for (FutureTask<Simulator> myTask : mySims) {
				Simulator myResult = myTask.get();

				myRequestsTotal = myRequestsTotal + myResult.getRequestTotal();
				myBreachesTotal = myBreachesTotal + myResult.getBreachTotal();
				
				System.out.println("Simulation complete: " + myResult.getRequestTotal() + " rq w/ " + myResult.getBreachTotal() +
					" throttled across " + myResult.getBreachedNodeTotal() + " nodes");
			}

			System.out.println();
			System.out.println("Total Requests: " + myRequestsTotal);
			System.out.println("Total Breaches: " + myBreachesTotal);
			System.out.println("Breaches vs Total %: " + ((double) myBreachesTotal / (double) myRequestsTotal) * 100);

			if (myBreachesTotal == 0)
				break;
			else {
				myCurrentThrottle += 5;
				System.out.println();
			}
		}

		myExecutor.shutdownNow();
	}

	private List<Long> generateDurations() {
		// Build request stream
		//
		List<Long> myRequestDurations = new ArrayList<>(REQUESTS_PER_SEC * RUN_TIME_IN_SECONDS.value(_options));
		Random myRandomizer = new Random();

		// Now, for each second, allocate the requests in that second according to the bucket percentages (could do this on a per minute
		// basis but if we did, a run time of less than a minute is tougher to implement).
		//
		for (int i = 0; i < RUN_TIME_IN_SECONDS.value(_options); i++) {
			for (int j = 0; j < MAX_CONTRIBUTING_BUCKET.value(_options); j++) {
				int baseTime = BUCKET_TIMES_MILLIS[j];
				int randomStep = BUCKET_TIMES_MILLIS[j+1] - baseTime;

				for (int k = 0; k < REQS_PER_BUCKET[j]; k++) {
					long myReqDuration = baseTime + myRandomizer.nextInt(randomStep + 1);
					myRequestDurations.add(new Long(myReqDuration));
				}
			}
		}

		Collections.shuffle(myRequestDurations);
		return myRequestDurations;		
	}

	private static class Simulator implements Callable<Simulator> {
		private final int _throttlePoint;
		private final List<Long> _requestDurations;
		private final int _totalServers;
		private long _requestTotal = 0;
		private long _breachTotal = 0;
		private int _breachedNodeCount = 0;

		Simulator(int aThrottlePoint, List<Long> aRequestDurations, int aTotalServers) {
			_throttlePoint = aThrottlePoint;
			_requestDurations = aRequestDurations;
			_totalServers = aTotalServers;
		}

		@Override
		public Simulator call() throws Exception {
			LB myBalancer = new LB(_totalServers,
					new ThrottlePolicy(_throttlePoint, 1000), REQUESTS_PER_SEC);

			myBalancer.allocate(_requestDurations);

			for (Node myNode : myBalancer.getNodes()) {
				// long myBreaches = myNode.getBreaches().size();
				long myBreaches = myNode.getBreachCount();

				_requestTotal += myNode.getRequestCount();

				if (myBreaches != 0) {
					_breachTotal += myBreaches;
					_breachedNodeCount++;
				}
			}

			return this;
		}

		public long getRequestTotal() {
			return _requestTotal;
		}

		public long getBreachTotal() {
			return _breachTotal;
		}

		public int getBreachedNodeTotal() {
			return _breachedNodeCount;
		}
	}

	private static class ThrottlePolicy {
		private final int _max;
		private final long _scopeMillis;

		ThrottlePolicy(int aMax, long aScopeMillis) {
			_max = aMax;
			_scopeMillis = aScopeMillis;
		}

		int getMax() {
			return _max;
		}

		long getScopeMillis() {
			return _scopeMillis;
		}
	}

	private static class LB {
		private final List<Node> _nodes = new ArrayList<>();
		private final int _reqsPerSec;
		private final double _reqsPerMillis;
		private final boolean _debug;

		LB(int aNumNodes, ThrottlePolicy aPolicy, int aReqsPerSec) {
			this(aNumNodes, aPolicy, aReqsPerSec, false);
		}

		LB(int aNumNodes, ThrottlePolicy aPolicy, int aReqsPerSec, boolean isDebug) {
			for (int i = 0; i < aNumNodes; i++)
				_nodes.add(new Node(i, aPolicy));

			_reqsPerSec = aReqsPerSec;
			_reqsPerMillis = _reqsPerSec / 1000;
			_debug = isDebug;
		}

		void allocate(List<Long> aRequestDurations) {
			long myCurrentTick = 0; // In seconds
			int myReqCount = 0;

			// Scatter the requests evenly across the seconds of runtime millisecond by millisecond
			//
			for (Long myDuration : aRequestDurations) {

				// Current time is a second + the request index for that second / the reqs per milli - allocate a request down to milliseconds
				//
				long myCurrentTime = (long) Math.floor((myCurrentTick * 1000) + (myReqCount / _reqsPerMillis));

				boolean myOutcome = findTargetNode(myCurrentTime).incomingRequest(myDuration, myCurrentTime);

				if (_debug)
					if (myOutcome)
						System.out.print("B");
					else
						System.out.print(".");

				++myReqCount;

				// If we're done with requests for this second, start on the next
				//
				if (myReqCount == _reqsPerSec) {
					++myCurrentTick;
					myReqCount = 0;
				}
			}
		}

		List<Node> getNodes() {
			return _nodes;
		}

		Node findTargetNode(long aCurrentTime) {
			SortedMap<Integer, Node> myNodes = new TreeMap<>();

			for (Node myNode : _nodes)
				myNodes.put(myNode.currentConnections(aCurrentTime), myNode);

			return myNodes.get(myNodes.firstKey());
		}
	}

	/*
		To simulate a customer specific load we'd need to generate a random load for them
		and then a further random background load to run alongside and in sum be the correct
		number of requests per second to follow the general trend

		We'd need each request to be labelled with a source, customer or general and then each
		node would need a list of limits per customer it tracks. Implication of this is we need
		to introduce a throttle class with a designation of source to apply to and what the limit is
		into node which would then maintain a list of requests against each throttle. This could be
		effected by offering the request to the throttle which can then decide to track it or not
		and be part of the cull cycle.
	*/

	private static class Node {
		private static class ValueComparator implements Comparator<Request> {
			interface Accessor {
				long getValue(Request aRequest);
			}

			private final Accessor _accessor;

			ValueComparator(Accessor anAccessor) {
				_accessor = anAccessor;
			}

			public int compare(Request a, Request b) {
				long test = _accessor.getValue(a) - _accessor.getValue(b);
				if (test < 0)
					return -1;
				else if (test > 0)
					return 1;
				else
					return 0;
			}
		}

		private final int _id;

		// Active requests (which can terminate millisecond by millisecond)
		//
		private final SortedSet<Request> _requests = 
			new TreeSet<>(new ValueComparator(new ValueComparator.Accessor() {
				public long getValue(Request aRequest) {
					return aRequest.getExpiry();
				}
			}));

		// Requests in scope of the throttles
		//
		private final SortedSet<Request> _inThrottleScope = 
			new TreeSet<>(new ValueComparator(new ValueComparator.Accessor() {
				public long getValue(Request aRequest) {
					return aRequest.getStartTime();
				}
			}));

		// Throttle breaches we've seen over the run
		//
		// private final List<Breach> _breaches = new LinkedList<>();
		private long _totalBreaches = 0;
		private long _totalRequests = 0;

		private final ThrottlePolicy _policy;

		Node(int anId, ThrottlePolicy aPolicy) {
			_id = anId;
			_policy = aPolicy;
		}

		int getId() {
			return _id;
		}

		List<Breach> getBreaches() {
			// return _breaches;
			throw new RuntimeException("Not supported");
		}

		long getBreachCount() {
			return _totalBreaches;
		}

		long getRequestCount() {
			return _totalRequests;
		}

		int currentConnections(long aCurrentTime) {
			cull(aCurrentTime);

			return _requests.size();
		}

		boolean incomingRequest(long aRequestDuration, long aCurrentTime) {
			++_totalRequests;
			cull(aCurrentTime);

			Request myReq = new Request(aRequestDuration, aCurrentTime);
			_requests.add(myReq);
			_inThrottleScope.add(myReq);

			if (_inThrottleScope.size() > _policy.getMax()) {
				//_breaches.add(new Breach(aCurrentTime, _requests.size(), _inThrottleScope.size(), _policy.getMax()));
				++_totalBreaches;
				return true;
			}

			return false;
		}

		private void cull(long aCurrentTime) {
			Iterator<Request> myRequests = _requests.iterator();

			while (myRequests.hasNext()) {
				Request myRequest = myRequests.next();

				// List is oldest to newest so first that hasn't expired means there will be no more
				//
				if (myRequest.hasExpired(aCurrentTime))
					myRequests.remove();
				else
					break;
			}

			myRequests = _inThrottleScope.iterator();

			while (myRequests.hasNext()) {
				Request myRequest = myRequests.next();

				// If a current time is more than throttle scope ahead of request start-time...
				// List is oldest to newest so first that hasn't expired means there will be no more
				//
				if (aCurrentTime - myRequest.getStartTime() >= _policy.getScopeMillis())
					myRequests.remove();
				else
					break;
			}
		}

		static class Breach {
			private final long _breachTime;
			private final int _queueSize;
			private final int _limit;
			private final int _throttleScope;

			Breach(long aTime, int aQueueSize, int aThrottleScope, int aLimit) {
				_breachTime = aTime;
				_queueSize = aQueueSize;
				_throttleScope = aThrottleScope;
				_limit = aLimit;
			}			

			public String toString() {
				return "Breach @ " + _breachTime + " with queue size " + _queueSize + 
					" of which in scope " + _throttleScope + " against limit " + _limit;
			}
		}

		private static class Request {
			private final long _expiry;
			private final long _startTime;

			Request(long aRequestDuration, long aCurrentTime) {
				_expiry = aRequestDuration + aCurrentTime;
				_startTime = aCurrentTime;
			}

			long getStartTime() {
				return _startTime;
			}

			long getExpiry() {
				return _expiry;
			}

			boolean hasExpired(long aCurrentTime) {
				return (aCurrentTime >= _expiry);
			}
		}
	}
}