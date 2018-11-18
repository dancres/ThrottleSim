import org.apache.commons.math3.random.RandomGenerator;

import java.util.ArrayList;
import java.util.List;

class BucketConsumer {
    private final List<Bucket> _buckets = new ArrayList<>();
    private final RandomGenerator _rng;

    BucketConsumer(Bucket[] aTemplateBuckets, RandomGenerator anRNG) {
        for (Bucket myB : aTemplateBuckets) {
            Bucket myBucket = myB.copy();

            _buckets.add(myBucket);
        }

        _rng = anRNG;
    }

    int nextSample() {
        int myChoice = _rng.nextInt(_buckets.size());
        Bucket myBucket = _buckets.get(myChoice);
        int myDuration = myBucket.draw(_rng);

        if (myBucket.numRemaining() == 0)
            _buckets.remove(myChoice);

        return myDuration;
    }

    boolean claim() {
        return _buckets.size() != 0;
    }
}
