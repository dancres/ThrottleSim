import org.apache.commons.math3.random.RandomGenerator;

public class CacheKeyBucket implements Bucket {
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
    public boolean isExhausted() {
        return (_remaining == 0);
    }

    @Override
    public Bucket copy() {
        return new CacheKeyBucket(_id, _remaining);
    }

    public String toString() {
        return super.toString() + String.format(" id %d will emit %d", _id, _remaining);
    }
}
