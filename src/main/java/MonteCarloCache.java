import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.SynchronizedRandomGenerator;
import org.apache.commons.math3.random.Well44497b;

import java.util.ArrayList;
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
        CompletionService<Sim> myCompletions = new ExecutorCompletionService<>(myExecutor);

        for (int i = 0; i < NUM_SIMS; i++) {
            Sim myTask = new Sim(NUM_CACHES, CACHE_SIZE, PROTOTYPE_BUCKETS, new Well44497b(_seeder.nextLong()));

            myCompletions.submit(myTask);
        }

        for (int i = 0; i < NUM_SIMS; i++) {
            Sim myResult = myCompletions.take().get();
            System.out.println("Hits: " + myResult.getHits() + " Misses: " + myResult.getMisses());
        }

        myExecutor.shutdownNow();
    }

    public static void main(String[] anArgs) throws Exception {
        new MonteCarloCache(anArgs).simulate();
    }

    private static class Sim implements Callable<Sim> {
        private final BucketConsumer _consumer;
        private final int _numCaches;
        private final ArrayList<Map<Integer, Integer>> _caches;
        private final int _cacheSize;
        private final RandomGenerator _rng;

        private int _misses = 0;
        private int _hits = 0;

        Sim(int aNumCaches, int aCacheSize, Bucket[] aBuckets, RandomGenerator aGen) {
            _rng = aGen;
            _consumer = new BucketConsumer(aBuckets, aGen);
            _cacheSize = aCacheSize;
            _numCaches = aNumCaches;
            _caches = new ArrayList<>(_numCaches);
            
            for (int i = 0; i < aNumCaches; i++)
                _caches.add(new LruCache<>(_cacheSize));
        }

        @Override
        public Sim call() {
            while (_consumer.claim()) {
                Map<Integer, Integer> myChoice = _caches.get(_rng.nextInt(_numCaches));
                Integer myKey = _consumer.nextSample();

                if (myChoice.get(myKey) == null) {
                    ++_misses;
                    myChoice.put(myKey, myKey);
                } else
                    ++_hits;
            }

            return this;
        }

        public int getHits() {
            return _hits;
        }

        public int getMisses() {
            return _misses;
        }
    }
}
