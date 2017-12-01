class ThrottlePolicy {
    private final int _max;
    private final long _scopeMillis;

    ThrottlePolicy(int aMax, long aScopeMillis) {
        _max = aMax;
        _scopeMillis = aScopeMillis;
    }

    int getMax() {
        return _max;
    }

    long getScopeMillis() {
        return _scopeMillis;
    }
}
