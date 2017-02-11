import java.util.*;

public class MonteCarloLB {
	// Percentage of requests that fall in 100ms ranges starting at 0ms
	//
	private static final double[] _tpBucketSizePercentages = {12.62, 25.58, 9.53, 7.04, 6.15, 5.42, 4.58, 3.74, 
		3.01, 2.44, 2.01, 1.70, 1.45, 1.25, 1.09, 0.97, 0.86, 0.77, 0.70, 0.64, 0.59, 0.65, 0.56, 0.50, 0.46,
		0.42, 0.38, 0.35, 0.32, 0.29, 0.26, 0.24, 0.22, 0.20, 0.19, 0.17, 0.16, 0.15, 0.13, 0.12, 0.11, 0.10, 
		0.10, 0.09, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.04 }; // % request  per bucket

	// Number of cycles to run per throttle setting
	//
	private static final int _simsPerSetting = 10;

	// How long each simulated cycle should be
	//
	private static final int _runTimeInSeconds = 60;

	// Requests per minute
	//
	private static final int _requestsPerMinute = 180000;
	private static final int _requestsPerSec = _requestsPerMinute / 60;

	// The initial setting for the per machine throttle
	//
	private static final int _throttleBase = 20;

	private static final int[] _tpBucketTimesMillis;
	private static final int[] _tpReqsPerBucket;

	static {
		_tpBucketTimesMillis = new int[_tpBucketSizePercentages.length + 1];
		_tpBucketTimesMillis[0] = 0;

		System.out.print("Bucket ceilings: ");

		for (int i = 0; i < _tpBucketSizePercentages.length; i++) {
			_tpBucketTimesMillis[1 + i] = 100 * (1 + i);
		}

		for (int i = 0; i < _tpBucketTimesMillis.length; i++)
			System.out.print(_tpBucketTimesMillis[i] + " ms ");

		System.out.println("");

		_tpReqsPerBucket = new int[_tpBucketSizePercentages.length];

		for (int i = 0; i < _tpBucketSizePercentages.length; i++) {
			int myReqs = (int) (_requestsPerSec * _tpBucketSizePercentages[i] / 100);
			_tpReqsPerBucket[i] = (myReqs == 0) ? 1 : myReqs;

			System.out.println(_tpReqsPerBucket[i] + " requests for bucket: " + i);
		}
	}

	public static void main(String[] anArgs) {
		/*
			To simulate a partner specific load we'd need to generate a random load for them
			and then a further random background load to run alongside and in sum be the correct
			number of requests per second to follow the general trend

			We'd need each request to be labelled with a source, partner or general and then each
			node would need a list of limits per partner it tracks. Implication of this is we need
			to introduce a throttle class with a designation of source to apply to and what the limit is
			into node which would then maintain a list of requests against each throttle. This could be
			effected by offering the request to the throttle which can then decide to track it or not
			and be part of the cull cycle.
		 */

		// Build request stream
		//
		List<Long> myRequestDurations = new ArrayList<>();
		Random myRandomizer = new Random();

		// Now, for each second, allocate the requests in that second according to the bucket percentages (could do this on a per minute
		// basis but if we did, a run time of less than a minute is tougher to implement).
		//
		System.out.println("Run-time (s): " + _runTimeInSeconds + " @ " + _requestsPerMinute + " rpm (" + _requestsPerSec + " rps)");
		for (int i = 0; i < _runTimeInSeconds; i++) {
			for (int j = 0; j < _tpBucketSizePercentages.length; j++) {
				int baseTime = _tpBucketTimesMillis[j];
				int randomStep = _tpBucketTimesMillis[j+1] - baseTime;

				for (int k = 0 ; k < _tpReqsPerBucket[j]; k++) {
					long myReqDuration = baseTime + myRandomizer.nextInt(randomStep + 1);
					myRequestDurations.add(new Long(myReqDuration));
				}
			}
		}

		System.out.println("Shuffle " + myRequestDurations.size() + " requests");
		Collections.shuffle(myRequestDurations);

		System.out.println("Run");

		int myCurrentThrottle = _throttleBase;

		while (true) {
			long myRequestsTotal = 0;
			long myBreachesTotal = 0;

			System.out.println("Throttle limit: " + myCurrentThrottle);

			for (int i = 0; i < _simsPerSetting; i++) {
				System.out.print(".");
				// Set up the network we wish to simulate and tell it how fast to run reqs per sec wise
				//
				LB myBalancer = new LB(150, new ThrottlePolicy(myCurrentThrottle, 1000), _requestsPerSec);

				myBalancer.allocate(myRequestDurations);

				for (Node myNode : myBalancer.getNodes()) {
					myRequestsTotal = myRequestsTotal + myNode.getRequestCount();
					myBreachesTotal = myBreachesTotal + myNode.getBreaches().size();
				}
			}

			System.out.println("");
			System.out.println("Total Requests: " + myRequestsTotal);
			System.out.println("Total Breaches: " + myBreachesTotal);

			if (myBreachesTotal == 0)
				break;
			else {
				myCurrentThrottle += 5;
				System.out.println("");
			}
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

	private static class Node {
		private final int _id;

		// Active requests (which can terminate millisecond by millisecond)
		//
		private final List<Request> _requests = new LinkedList<>();

		// Requests in scope of the throttles for throttle scope
		//
		private final List<Request> _inThrottleScope = new LinkedList<>();

		// Number of throttle breaches we've seen over the run
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