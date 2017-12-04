class Request {
    private final long _expiry;
    private final long _startTime;

    Request(int aRequestDuration, long aCurrentTime) {
        _expiry = aRequestDuration + aCurrentTime;
        _startTime = aCurrentTime;
    }

    long getStartTime() {
        return _startTime;
    }

    long getExpiry() {
        return _expiry;
    }

    boolean hasExpired(long aCurrentTime) {
        return (aCurrentTime >= _expiry);
    }
}
