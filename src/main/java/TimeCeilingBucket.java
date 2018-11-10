import org.apache.commons.math3.random.RandomGenerator;

class TimeCeilingBucket implements Bucket {
    private final int _baseTime;
    private final double _reqsPercentage;
    private final int _reqCount;
    private int _remainingRequests;

    TimeCeilingBucket(int aTimeCeiling, double aReqsPercent, int aTotalRequests) {
        this(aTimeCeiling - 100, aReqsPercent, (int) (aTotalRequests * aReqsPercent / 100),
                (int) (aTotalRequests * aReqsPercent / 100));
    }

    private TimeCeilingBucket(int aBaseTime, double aReqsPercent, int aReqCount, int aRemaining) {
        _baseTime = aBaseTime;
        _reqsPercentage = aReqsPercent;
        _reqCount = aReqCount;
        _remainingRequests = aRemaining;
    }

    @Override
    public int draw(RandomGenerator anRNG) {
        if (_remainingRequests == 0)
            throw new IllegalStateException();
        else
            --_remainingRequests;

        return _baseTime + anRNG.nextInt(100);
    }

    @Override
    public int numRemaining() {
        return _remainingRequests;
    }

    @Override
    public Bucket copy() {
        return new TimeCeilingBucket(_baseTime, _reqsPercentage,
                _reqCount, _remainingRequests);
    }

    public String toString() {
        return super.toString() +
                String.format(" at %d ms w/ (%.4g%%) = %d", _baseTime, _reqsPercentage, _reqCount);
    }
}
