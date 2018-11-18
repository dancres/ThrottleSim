import org.apache.commons.math3.random.RandomGenerator;

class FixedDurationBucket implements Bucket<Integer> {
    private final int _duration;
    private final int _totalRequests;
    private int _remainingRequests;

    FixedDurationBucket(int aDuration, int aTotalRequests) {
        _duration = aDuration;
        _totalRequests = aTotalRequests;
        _remainingRequests = _totalRequests;
    }

    @Override
    public Integer draw(RandomGenerator anRNG) {
        if (_remainingRequests == 0)
            throw new IllegalStateException();
        else
            --_remainingRequests;

        return _duration;
    }


    @Override
    public int numRemaining() {
        return _remainingRequests;
    }
    
    @Override
    public Bucket<Integer> copy() {
        return new FixedDurationBucket(_duration, _totalRequests);
    }
}
