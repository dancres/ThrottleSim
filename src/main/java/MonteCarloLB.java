import java.util.*;
import java.util.concurrent.*;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.SynchronizedRandomGenerator;
import org.apache.commons.math3.random.Well44497b;

public class MonteCarloLB implements DurationProducer {
	// Percentage of requests that fall in 100ms ranges starting at 0-100ms (long-tailed distribution so trimmed & not summing to 100%)
	//
	private static final double[] BUCKET_SIZE_PERCENTAGES = {12.62, 25.58, 9.53, 7.04, 6.15, 5.42, 4.58, 3.74,
			3.01, 2.44, 2.01, 1.70, 1.45, 1.25, 1.09, 0.97, 0.86, 0.77, 0.70, 0.64, 0.59, 0.65, 0.56, 0.50, 0.46,
			0.42, 0.38, 0.35, 0.32, 0.29, 0.26, 0.24, 0.22, 0.20, 0.19, 0.17, 0.16, 0.15, 0.13, 0.12, 0.11, 0.10,
			0.10, 0.09, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.04, 0.03, 0.03, 0.03,
			0.03, 0.02, 0.02, 0.02, 0.02, 0.01, 0.01, 0.01, 0.01, 0.01, 0.005, 0.005, 0.005, 0.005, 0.005};
	
	// Number of cores to use for simulations
	//
	private final Integer NUM_CORES;

	// Number of cycles to run per throttle setting
	//
	private final Integer SIMS_PER_SETTING;

	// How long each simulated cycle should be
	//
	private final Integer RUN_TIME_IN_SECONDS;

	// How many buckets in the distribution to use
	//
	private final Integer MAX_CONTRIBUTING_BUCKET;

	// Requests per minute
	//
	private final Integer REQUESTS_PER_MINUTE;
	private final int REQUESTS_PER_SEC;

	// Dump node by node stats
	private final Boolean NODE_STATS;

	// Debug mode
	//
	private final Boolean DEBUG_MODE;

	// The initial setting for the per machine throttle
	//
	private final Integer THROTTLE_BASE;

	// The increment post-breach, to apply to throttle for each round of simulations
	//
	private final Integer THROTTLE_INCR;

	// Number of machines in the cluster
	//
	private final Integer TOTAL_NODES;

	private final int[] BUCKET_TIMES_MILLIS;
	private final int[] REQS_PER_BUCKET;

	private final RandomGenerator _seeder = new SynchronizedRandomGenerator(new Well44497b());

	private static class Configuration {
		private final OptionParser myOp = new OptionParser();

		final OptionSpec<Integer> _numCoresParam = myOp.accepts("c").withOptionalArg().ofType(Integer.class).defaultsTo(2);
		final OptionSpec<Integer> _simsPerSettingParam = myOp.accepts("s").withOptionalArg().ofType(Integer.class).defaultsTo(12);
		final OptionSpec<Integer> _runTimeInSecondsParam = myOp.accepts("t").withOptionalArg().ofType(Integer.class).defaultsTo(60);
		final OptionSpec<Integer> _maxContributingBucket = myOp.accepts("b").withOptionalArg().ofType(Integer.class).defaultsTo(BUCKET_SIZE_PERCENTAGES.length);
		final OptionSpec<Integer> _requestsPerMinParam = myOp.accepts("r").withOptionalArg().ofType(Integer.class).defaultsTo(160000);
		final OptionSpec<Integer> _throttleBaseParam = myOp.accepts("l").withOptionalArg().ofType(Integer.class).defaultsTo(25);
		final OptionSpec<Integer> _throttleIncrParam = myOp.accepts("i").withOptionalArg().ofType(Integer.class).defaultsTo(5);
		final OptionSpec<Integer> _totalNodesParam = myOp.accepts("h").withOptionalArg().ofType(Integer.class).defaultsTo(200);
		final OptionSpec<Boolean> _debugModeParam = myOp.accepts("d").withOptionalArg().ofType(Boolean.class).defaultsTo(false);
		final OptionSpec<Boolean> _nodeStats = myOp.accepts("ns").withOptionalArg().ofType(Boolean.class).defaultsTo(false);

		OptionSet produce(String[] anArgs) {
			return myOp.parse(anArgs);
		}
	}

	private MonteCarloLB(String[] anArgs) {
		Configuration myConfig = new Configuration();
		OptionSet myOptions = myConfig.produce(anArgs);

		NUM_CORES = myConfig._numCoresParam.value(myOptions);
		SIMS_PER_SETTING = myConfig._simsPerSettingParam.value(myOptions);
		RUN_TIME_IN_SECONDS = myConfig._runTimeInSecondsParam.value(myOptions);
		MAX_CONTRIBUTING_BUCKET = myConfig._maxContributingBucket.value(myOptions);
		REQUESTS_PER_MINUTE = myConfig._requestsPerMinParam.value(myOptions);
		REQUESTS_PER_SEC = REQUESTS_PER_MINUTE / 60;
		THROTTLE_BASE = myConfig._throttleBaseParam.value(myOptions);
		THROTTLE_INCR = myConfig._throttleIncrParam.value(myOptions);
		TOTAL_NODES = myConfig._totalNodesParam.value(myOptions);
		DEBUG_MODE = myConfig._debugModeParam.value(myOptions);
		NODE_STATS = myConfig._nodeStats.value(myOptions);

		BUCKET_TIMES_MILLIS = computeBucketCeilingTimes();

		REQS_PER_BUCKET = computeRequestsPerBucket();
	}

	private int[] computeBucketCeilingTimes() {
		int[] myBucketTimeCeilings = new int[MAX_CONTRIBUTING_BUCKET + 1];
		myBucketTimeCeilings[0] = 0;

		System.out.print("Bucket ceilings: ");

		for (int i = 1; i <= MAX_CONTRIBUTING_BUCKET; i++) {
			myBucketTimeCeilings[i] = 100 * i;
		}

		for (int i = 1; i < myBucketTimeCeilings.length; i++)
			System.out.print(myBucketTimeCeilings[i] + " ms ");

		System.out.println();
		System.out.println();

		return myBucketTimeCeilings;
	}

	private int[] computeRequestsPerBucket() {
		System.out.print("Request distribution: ");

		int[] myReqsPerBucket = new int[MAX_CONTRIBUTING_BUCKET];
		double myBucketPercentTotal = 0.0;

		for (int i = 0; i < MAX_CONTRIBUTING_BUCKET; i++) {
			myBucketPercentTotal += BUCKET_SIZE_PERCENTAGES[i];
			int myReqs = (int) (RUN_TIME_IN_SECONDS * REQUESTS_PER_SEC * BUCKET_SIZE_PERCENTAGES[i] / 100);
			myReqsPerBucket[i] = (myReqs == 0) ? 1 : myReqs;

			System.out.format("(" + i + ") " + myReqsPerBucket[i] + " requests (%.4g%%) ", myBucketPercentTotal);
		}

		System.out.println();
		System.out.println();
		
		return myReqsPerBucket;
	}

	public static void main(String[] anArgs) throws Exception {
		new MonteCarloLB(anArgs).simulate();
	}

	private void simulate() throws Exception {
		System.out.println("Run-time (s): " + RUN_TIME_IN_SECONDS + " @ " +
			REQUESTS_PER_MINUTE + " rpm (" + REQUESTS_PER_SEC + " rps)");
		System.out.println("Cores: " + NUM_CORES);

		ThreadPoolExecutor myExecutor = new ThreadPoolExecutor(NUM_CORES, NUM_CORES, 30,
				TimeUnit.SECONDS, new LinkedBlockingQueue<>());
		CompletionService<Simulator> myCompletions = new ExecutorCompletionService<>(myExecutor);

		int myCurrentThrottle = THROTTLE_BASE;
		long myBreachesTotal;

		do {
			long myRequestsTotal = 0;
			myBreachesTotal = 0;

			System.out.println("Throttle limit: " + myCurrentThrottle + " requests per server of which there are: " +
					TOTAL_NODES);

			for (int i = 0; i < SIMS_PER_SETTING; i++) {
				Simulator myTask = new Simulator(DEBUG_MODE, this, REQUESTS_PER_SEC,
						new LB(TOTAL_NODES, new ThrottlePolicy(myCurrentThrottle, 1000),
								DEBUG_MODE));

				myCompletions.submit(myTask);
			}

			for (int i = 0; i < SIMS_PER_SETTING; i++) {
				Simulator myResult = myCompletions.take().get();

				myRequestsTotal += myResult.getRequestTotal();
				myBreachesTotal += myResult.getBreachTotal();
				
				System.out.println("Simulation complete: " + myResult.getRequestTotal() + " rq w/ " + myResult.getBreachTotal() +
					" throttled across " + myResult.getBreachedNodeTotal() + " nodes");

				if (NODE_STATS) {
					for (Node.SimDetails myDetails : myResult.simDetailsByNode())
						System.out.println(myDetails);

					System.out.println();
				}

				if (DEBUG_MODE) {
					myResult.getBreachDetail().forEach((id, breaches) -> {
						System.out.println("Node Id: " + id);
						
						for (Node.Breach myBreach : breaches)
							System.out.print(myBreach);

						System.out.println();
					});
				}
			}

			System.out.println();
			System.out.println("Total Requests: " + myRequestsTotal);
			System.out.println("Total Breaches: " + myBreachesTotal);
			System.out.format("Breaches vs Total: %% %.6g\n", ((double) myBreachesTotal / (double) myRequestsTotal) * 100);

			myCurrentThrottle += THROTTLE_INCR;
			System.out.println();
			
		} while (myBreachesTotal != 0);

		myExecutor.shutdownNow();
	}

	public List<Integer> produce() {
		// Build request stream
		//
		List<Integer> myRequestDurations = new ArrayList<>(REQUESTS_PER_SEC * RUN_TIME_IN_SECONDS);
		Well44497b myRandomizer = new Well44497b(_seeder.nextLong());

		// Now, for each second, allocate the requests in that second according to the bucket percentages (could do this on a per minute
		// basis but if we did, a run time of less than a minute is tougher to implement).
		//
		for (int j = 0; j < MAX_CONTRIBUTING_BUCKET; j++) {
			int baseTime = BUCKET_TIMES_MILLIS[j];
			int randomStep = BUCKET_TIMES_MILLIS[j+1] - baseTime;

			for (int k = 0; k < REQS_PER_BUCKET[j]; k++) {
				int myReqDuration = baseTime + myRandomizer.nextInt(randomStep + 1);
				myRequestDurations.add(myReqDuration);
			}
		}

		Collections.shuffle(myRequestDurations);
		return myRequestDurations;		
	}
}