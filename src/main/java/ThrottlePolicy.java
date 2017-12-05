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

    boolean outOfScope(Request aRequest, long aCurrentTime) {
        // If a current time is more than throttle scope ahead of request start-time...
        // List is oldest to newest so first that hasn't expired means there will be no more
        //
        return ((aCurrentTime / getScopeMillis()) > (aRequest.getStartTime() / getScopeMillis()));
    }
}
