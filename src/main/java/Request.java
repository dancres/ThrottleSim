class Request {
    private static long _nextId = 0;
    private final long _expiry;
    private final long _startTime;
    private final long _id = _nextId++;

    Request(int aRequestDuration, long aCurrentTime) {
        _expiry = aRequestDuration + aCurrentTime;
        _startTime = aCurrentTime;
    }

    long getId() {
        return _id;
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
