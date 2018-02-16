import org.apache.commons.math3.random.RandomGenerator;

import java.util.ArrayList;
import java.util.List;

public class BucketConsumer {
    private final List<Bucket> _buckets = new ArrayList<>();
    private final RandomGenerator _rng;

    BucketConsumer(Bucket[] aTemplateBuckets, RandomGenerator anRNG) {
        for (Bucket myB : aTemplateBuckets)
            _buckets.add(myB.copy());

        _rng = anRNG;
    }

    int nextDuration() {
        if (_buckets.size() == 0) {
            throw new IllegalStateException();
        }
        
        int myChoice = _rng.nextInt(_buckets.size());
        Bucket myBucket = _buckets.get(myChoice);
        int myDuration = myBucket.draw(_rng);

        if (myBucket.isExhausted())
            _buckets.remove(myChoice);

        return myDuration;
    }

    boolean hasNext() {
        return _buckets.size() != 0;
    }
}