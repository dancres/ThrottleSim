import org.apache.commons.math3.random.RandomGenerator;

class CacheKeyBucket implements Bucket {
    private int _id;
    private int _remaining;

    CacheKeyBucket(int myId, int myNumCases) {
        _id = myId;
        _remaining = myNumCases;
    }

    @Override
    public int draw(RandomGenerator anRNG) {
        --_remaining;

        return _id;
    }

    @Override
    public int numRemaining() {
        return _remaining;
    }

    @Override
    public Bucket copy() {
        return new CacheKeyBucket(_id, _remaining);
    }

    public String toString() {
        return super.toString() + String.format(" id %d will emit %d", _id, _remaining);
    }
}
