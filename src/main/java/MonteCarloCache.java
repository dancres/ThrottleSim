import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.SynchronizedRandomGenerator;
import org.apache.commons.math3.random.Well44497b;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class MonteCarloCache {
    private final RandomGenerator _seeder = new SynchronizedRandomGenerator(new Well44497b());

    private final int NUM_CORES;
    private final int NUM_SIMS;
    private final int NUM_KEYS;
    private final int NUM_CACHES;
    private final int CACHE_SIZE;
    private final int SCALE;

    private final Bucket[] PROTOTYPE_BUCKETS;

    private static class Configuration {
        private final OptionParser myOp = new OptionParser();

        final OptionSpec<Integer> _numCoresParam = myOp.accepts("c").withOptionalArg().ofType(Integer.class).defaultsTo(2);
        final OptionSpec<Integer> _numSims = myOp.accepts("s").withOptionalArg().ofType(Integer.class).defaultsTo(12);
        final OptionSpec<Integer> _numKeys = myOp.accepts("k").withOptionalArg().ofType(Integer.class).defaultsTo(100000);
        final OptionSpec<Integer> _numCaches = myOp.accepts("h").withOptionalArg().ofType(Integer.class).defaultsTo(12);
        final OptionSpec<Integer> _cacheSize = myOp.accepts("z").withOptionalArg().ofType(Integer.class).defaultsTo(67000);
        final OptionSpec<Integer> _cycles = myOp.accepts("n").withOptionalArg().ofType((Integer.class)).defaultsTo(1);

        OptionSet produce(String[] anArgs) {
            return myOp.parse(anArgs);
        }
    }

    private MonteCarloCache(String [] anArgs) {
        Configuration myConfig = new Configuration();
        OptionSet myOptions = myConfig.produce(anArgs);

        NUM_CORES = myConfig._numCoresParam.value(myOptions);
        NUM_SIMS = myConfig._numSims.value(myOptions);
        NUM_KEYS = myConfig._numKeys.value(myOptions);
        NUM_CACHES = myConfig._numCaches.value(myOptions);
        CACHE_SIZE = myConfig._cacheSize.value(myOptions);
        SCALE = myConfig._cycles.value(myOptions);

        PROTOTYPE_BUCKETS = new CacheKeyBucket[NUM_KEYS];

        for (int i = 0; i < NUM_KEYS; i++) {
            PROTOTYPE_BUCKETS[i] = new CacheKeyBucket(i + 1, getContribution(i + 1));
        }
    }

    private int getContribution(int aRank) {
        double a = 2.107e+04;
        double b = -0.77;

        return Math.max(Double.valueOf(a * Math.pow(aRank, b)).intValue() * SCALE, 1);
    }

    private void simulate() throws Exception {
        System.out.println("Sims: " + NUM_SIMS + " for total keys " + NUM_KEYS + " @ cache size " +
                CACHE_SIZE + " w/ " + NUM_CACHES + " caches at scale: " + SCALE);

        System.out.println("Cores: " + NUM_CORES);

        ThreadPoolExecutor myExecutor = new ThreadPoolExecutor(NUM_CORES, NUM_CORES, 30,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        for (int i = 0; i < NUM_SIMS; i++) {
            Sim myTask = new Sim(myExecutor, NUM_CACHES, CACHE_SIZE, PROTOTYPE_BUCKETS, _seeder.nextLong());

            myTask.invoke();
            System.out.println("Hits: " + myTask.getHits() + " Misses: " + myTask.getMisses());
        }

        myExecutor.shutdownNow();
    }

    public static void main(String[] anArgs) throws Exception {
        new MonteCarloCache(anArgs).simulate();
    }

    private static class Sim {
        private static final int BATCH_SIZE = 50;

        private final BucketConsumer _consumer;
        private final List<Map<Integer, Integer>> _caches;
        private final int _cacheSize;
        private final RandomGenerator _rng;
        private final CompletionService<Requester> _completions;

        private int _hits;
        private int _misses;
        private int _taskCount;

        Sim(ThreadPoolExecutor anExec, int aNumCaches, int aCacheSize, Bucket[] aBuckets, long aSeed) {
            _rng = new Well44497b(aSeed);
            _consumer = new BucketConsumer(aBuckets, _rng);
            _cacheSize = aCacheSize;
            _completions = new ExecutorCompletionService<>(anExec);

            ArrayList<Map<Integer, Integer>> myCaches = new ArrayList<>();

            for (int i = 0; i < aNumCaches; i++)
                myCaches.add(new LruCache<>(_cacheSize));

            _caches = Collections.unmodifiableList(myCaches);
        }

        void invoke() throws Exception {
            List<Integer> mySamples = new ArrayList<>(BATCH_SIZE);

            while (_consumer.claim()) {
                mySamples.add(_consumer.nextSample());

                if (mySamples.size() == BATCH_SIZE) {
                    dispatch(mySamples);
                    mySamples = new ArrayList<>(BATCH_SIZE);
                    consume();
                }
            }

            dispatch(mySamples);
            
            for (int i = 0; i < _taskCount; i++)
                consume(_completions.take().get());
        }

        private void dispatch(List<Integer> aSamples) {
            _completions.submit(new Requester(aSamples, _rng.nextLong()));
            ++_taskCount;
        }

        private void consume() throws Exception {
            Future<Requester> myF = _completions.poll();

            if (myF != null) {
                consume(myF.get());
                --_taskCount;
            }
        }

        private void consume(Requester aReq) {
            _misses += aReq.getMiss();
            _hits += aReq.getHit();
        }

        int getHits() {
            return _hits;
        }

        int getMisses() {
            return _misses;
        }

        private class Requester implements Callable<Requester> {
            private final List<Integer> _keys;
            private RandomGenerator _randomizer;
            private int _hit;
            private int _miss;

            Requester(List<Integer> aKeys, long aSeed) {
                _keys = aKeys;
                _randomizer = new Well44497b(aSeed);
            }

            @Override
            public Requester call() {
                _keys.forEach(k -> {
                    Map<Integer, Integer> myChoice = _caches.get(_randomizer.nextInt(_caches.size()));

                    boolean didMiss;

                    synchronized (myChoice) {
                        didMiss = (myChoice.putIfAbsent(k, k) == null);
                    }

                    if (didMiss)
                        ++_miss;
                    else
                        ++_hit;
                });

                tidyUp();
                return this;
            }

            private void tidyUp() {
                _keys.clear();
                _randomizer = null;
            }

            private int getHit() {
                return _hit;
            }

            private int getMiss() {
                return _miss;
            }
        }
    }
}
