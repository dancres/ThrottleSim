import org.apache.commons.math3.random.RandomGenerator;

class FixedDurationBucket implements Bucket {
    private int _duration;
    private int _totalRequests;
    private int _remainingRequests;

    FixedDurationBucket(int aDuration, int aTotalRequests) {
        _duration = aDuration;
        _totalRequests = aTotalRequests;
        _remainingRequests = _totalRequests;
    }

    @Override
    public int draw(RandomGenerator anRNG) {
        if (_remainingRequests == 0)
            throw new IllegalStateException();
        else
            --_remainingRequests;

        return _duration;
    }

    @Override
    public boolean isExhausted() {
        return _remainingRequests == 0;
    }

    @Override
    public Bucket copy() {
        return new FixedDurationBucket(_duration, _totalRequests);
    }
}
