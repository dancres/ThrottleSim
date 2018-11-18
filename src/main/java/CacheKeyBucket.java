import org.apache.commons.math3.random.RandomGenerator;
import sample.Bucket;

class CacheKeyBucket implements Bucket<Integer> {
    private final int _id;
    private int _remaining;

    CacheKeyBucket(int myId, int myNumCases) {
        _id = myId;
        _remaining = myNumCases;
    }

    @Override
    public Integer draw(RandomGenerator anRNG) {
        --_remaining;

        return _id;
    }

    @Override
    public int numRemaining() {
        return _remaining;
    }

    @Override
    public Bucket<Integer> copy() {
        return new CacheKeyBucket(_id, _remaining);
    }

    public String toString() {
        return super.toString() + String.format(" id %d will emit %d", _id, _remaining);
    }
}
