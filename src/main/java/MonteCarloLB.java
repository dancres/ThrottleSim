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
	private static int NUM_CORES;

	// Number of cycles to run per throttle setting
	//
	private static int SIMS_PER_SETTING;

	// How long each simulated cycle should be
	//
	private static int RUN_TIME_IN_SECONDS;

	// How many buckets in the distribution to use
	//
	private static int MAX_CONTRIBUTING_BUCKET;

	// Requests per minute
	//
	private static int REQUESTS_PER_MINUTE;
	private static int REQUESTS_PER_SEC;

	// The initial setting for the per machine throttle
	//
	private static int THROTTLE_BASE;

	// Number of machines in the cluster
	//
	private static int TOTAL_SERVERS;

	private static int[] BUCKET_TIMES_MILLIS;
	private static int[] REQS_PER_BUCKET;

	private static void init(String[] anArgs) {
		OptionParser myOp = new OptionParser();

		OptionSpec<Integer> myCores = myOp.accepts("c").withOptionalArg().ofType(Integer.class).defaultsTo(2);
		OptionSpec<Integer> mySims = myOp.accepts("s").withOptionalArg().ofType(Integer.class).defaultsTo(12);
		OptionSpec<Integer> myTime = myOp.accepts("t").withOptionalArg().ofType(Integer.class).defaultsTo(60);
		OptionSpec<Integer> myBucket = myOp.accepts("b").withOptionalArg().ofType(Integer.class).defaultsTo(BUCKET_SIZE_PERCENTAGES.length);
		OptionSpec<Integer> myRate = myOp.accepts("r").withOptionalArg().ofType(Integer.class).defaultsTo(160000);
		OptionSpec<Integer> myLimit = myOp.accepts("l").withOptionalArg().ofType(Integer.class).defaultsTo(25);
		OptionSpec<Integer> myHosts = myOp.accepts("h").withOptionalArg().ofType(Integer.class).defaultsTo(200);

		OptionSet myOptions = myOp.parse(anArgs);

		NUM_CORES = myCores.value(myOptions);
		SIMS_PER_SETTING = mySims.value(myOptions);
		RUN_TIME_IN_SECONDS = myTime.value(myOptions);
		MAX_CONTRIBUTING_BUCKET = myBucket.value(myOptions);
		REQUESTS_PER_MINUTE = myRate.value(myOptions);
		THROTTLE_BASE = myLimit.value(myOptions);
		TOTAL_SERVERS = myHosts.value(myOptions);

 		REQUESTS_PER_SEC = REQUESTS_PER_MINUTE / 60;

		BUCKET_TIMES_MILLIS = new int[MAX_CONTRIBUTING_BUCKET + 1];
		BUCKET_TIMES_MILLIS[0] = 0;

		System.out.print("Bucket ceilings: ");

		for (int i = 0; i < MAX_CONTRIBUTING_BUCKET; i++) {
			BUCKET_TIMES_MILLIS[1 + i] = 100 * (1 + i);
		}

		for (int i = 1; i < BUCKET_TIMES_MILLIS.length; i++)
			System.out.print(BUCKET_TIMES_MILLIS[i] + " ms ");

		System.out.println();
		System.out.println();
		System.out.print("Request distribution: ");

		REQS_PER_BUCKET = new int[MAX_CONTRIBUTING_BUCKET];

		for (int i = 0; i < MAX_CONTRIBUTING_BUCKET; i++) {
			int myReqs = (int) (REQUESTS_PER_SEC * BUCKET_SIZE_PERCENTAGES[i] / 100);
			REQS_PER_BUCKET[i] = (myReqs == 0) ? 1 : myReqs;

			System.out.print("(" + i + ") " + REQS_PER_BUCKET[i] + " requests ");
		}

		System.out.println();
		System.out.println();
	}

	public static void main(String[] anArgs) throws Exception {
		init(anArgs);

		System.out.println("Run-time (s): " + RUN_TIME_IN_SECONDS + " @ " + REQUESTS_PER_MINUTE + " rpm (" + REQUESTS_PER_SEC + " rps)");
		System.out.println("Cores: " + NUM_CORES);

		ThreadPoolExecutor myExecutor = new ThreadPoolExecutor(NUM_CORES, NUM_CORES, 30,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

		int myCurrentThrottle = THROTTLE_BASE;
		
		while (true) {
			long myRequestsTotal = 0;
			long myBreachesTotal = 0;
			LinkedList<FutureTask<Simulator>> mySims = new LinkedList<>();

			System.out.println("Throttle limit: " + myCurrentThrottle + " per server of which there are: " + TOTAL_SERVERS);

			for (int i = 0; i < SIMS_PER_SETTING; i++) {
				FutureTask<Simulator> myTask =
						new FutureTask<>(new Simulator(myCurrentThrottle, generateDurations()));
				mySims.add(myTask);
				myExecutor.execute(myTask);
			}

			for (FutureTask<Simulator> myTask : mySims) {
				Simulator myResult = myTask.get();

				myRequestsTotal = myRequestsTotal + myResult.getRequestTotal();
				myBreachesTotal = myBreachesTotal + myResult.getBreachTotal();
				System.out.print(".");
			}

			System.out.println();
			System.out.println("Total Requests: " + myRequestsTotal);
			System.out.println("Total Breaches: " + myBreachesTotal);

			if (myBreachesTotal == 0)
				break;
			else {
				myCurrentThrottle += 5;
				System.out.println();
			}
		}

		myExecutor.shutdownNow();
	}

	private static List<Long> generateDurations() {
		// Build request stream
		//
		List<Long> myRequestDurations = new ArrayList<>();
		Random myRandomizer = new Random();

		// Now, for each second, allocate the requests in that second according to the bucket percentages (could do this on a per minute
		// basis but if we did, a run time of less than a minute is tougher to implement).
		//
		for (int i = 0; i < RUN_TIME_IN_SECONDS; i++) {
			for (int j = 0; j < MAX_CONTRIBUTING_BUCKET; j++) {
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
		private long _requestTotal = 0;
		private long _breachTotal = 0;

		Simulator(int aThrottlePoint, List<Long> aRequestDurations) {
			_throttlePoint = aThrottlePoint;
			_requestDurations = aRequestDurations;
		}

		@Override
		public Simulator call() throws Exception {
			LB myBalancer = new LB(TOTAL_SERVERS,
					new ThrottlePolicy(_throttlePoint, 1000), REQUESTS_PER_SEC);

			myBalancer.allocate(_requestDurations);

			for (Node myNode : myBalancer.getNodes()) {
				_requestTotal += myNode.getRequestCount();
				_breachTotal += myNode.getBreaches().size();
			}

			return this;
		}

		public long getRequestTotal() {
			return _requestTotal;
		}

		public long getBreachTotal() {
			return _breachTotal;
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
			int myMinConnections = Integer.MAX_VALUE;
			Node myFavouredNode = null;

			// Allocate to nodes based on least connections
			//
			for (Node myNode : _nodes)
				if (myNode.currentConnections(aCurrentTime) < myMinConnections) {
					myFavouredNode = myNode;
					myMinConnections = myNode.currentConnections(aCurrentTime);
				}

			return myFavouredNode;
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
		private final int _id;

		// Active requests (which can terminate millisecond by millisecond)
		//
		private final List<Request> _requests = new LinkedList<>();

		// Requests in scope of the throttles
		//
		private final List<Request> _inThrottleScope = new LinkedList<>();

		// Throttle breaches we've seen over the run
		//
		private final List<Breach> _breaches = new LinkedList<>();
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
			return _breaches;
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
				_breaches.add(new Breach(aCurrentTime, _requests.size(), _inThrottleScope.size(), _policy.getMax()));
				return true;
			}

			return false;
		}

		private void cull(long aCurrentTime) {
			Iterator<Request> myRequests = _requests.iterator();

			while (myRequests.hasNext()) {
				Request myRequest = myRequests.next();

				if (myRequest.hasExpired(aCurrentTime))
					myRequests.remove();
			}

			myRequests = _inThrottleScope.iterator();

			while (myRequests.hasNext()) {
				Request myRequest = myRequests.next();

				// If a current time is more than throttle scope ahead of request start-time...
				//
				if (aCurrentTime - myRequest.getStartTime() >= _policy.getScopeMillis()) {
					myRequests.remove();
				}
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

			boolean hasExpired(long aCurrentTime) {
				return (aCurrentTime >= _expiry);
			}
		}
	}
}