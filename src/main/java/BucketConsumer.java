import org.apache.commons.math3.random.RandomGenerator;

import java.util.ArrayList;
import java.util.List;

class BucketConsumer<T> {
    private final List<Bucket<T>> _buckets = new ArrayList<>();
    private final RandomGenerator _rng;

    BucketConsumer(Bucket<T>[] aTemplateBuckets, RandomGenerator anRNG) {
        for (Bucket<T> myB : aTemplateBuckets) {
            Bucket<T> myBucket = myB.copy();

            _buckets.add(myBucket);
        }

        _rng = anRNG;
    }

    T nextSample() {
        int myChoice = _rng.nextInt(_buckets.size());
        Bucket<T> myBucket = _buckets.get(myChoice);
        T mySample = myBucket.draw(_rng);

        if (myBucket.numRemaining() == 0)
            _buckets.remove(myChoice);

        return mySample;
    }

    boolean claim() {
        return _buckets.size() != 0;
    }
}
