import java.util.*;

/*
    To simulate a customer specific load we'd need to generate a random load for them
    and then a further random background load to run alongside and in sum be the correct
    number of requests per second to follow the general trend

    We'd need each request to be labelled with a source, customer or general and then each
    node would need a list of limits per customer it tracks. Implication of this is we need
    to introduce a throttle class with a designation of source to apply to and what the limit is
    into node which would then maintain a list of requests against each throttle. This could be
    effected by offering the request to the throttle which can then decide to track it or not
    and be part of the cull cycle.
*/
class Node {
    private static class ValueComparator implements Comparator<Request> {
        interface Accessor {
            long getValue(Request aRequest);
        }

        private final Accessor _accessor;

        ValueComparator(Accessor anAccessor) {
            _accessor = anAccessor;
        }

        public int compare(Request a, Request b) {
            long test = _accessor.getValue(a) - _accessor.getValue(b);

            // If the requests are equal on the given attribute, fall back to their id to assert an order
            //
            if (test == 0)
                return normalize(a.getId() - b.getId());
            else
                return normalize(test);
        }

        private int normalize(long aComparison) {
            if (aComparison < 0)
                return -1;
            else if (aComparison > 0)
                return 1;
            else {
                return 0;
            }
        }
    }

    private final int _id;

    // Active requests (which can terminate millisecond by millisecond)
    //
    private final SortedSet<Request> _requests =
            new TreeSet<>(new ValueComparator(Request::getExpiry));

    // Requests in scope of the throttles
    //
    private final Queue<Request> _inThrottleScope = new LinkedList<>();

    private long _totalBreaches = 0;
    private long _totalRequests = 0;

    private final ThrottlePolicy _policy;
    private final boolean _recordBreaches;

    // Throttle breaches we've seen over the run
    //
    private final List<Breach> _breaches = new LinkedList<>();

    Node(int anId, ThrottlePolicy aPolicy, boolean shouldRecordBreaches) {
        _id = anId;
        _policy = aPolicy;
        _recordBreaches = shouldRecordBreaches;
    }

    int getId() {
        return _id;
    }

    List<Breach> getBreaches() {
        if (! _recordBreaches)
            throw new IllegalStateException(
                    "To recover breach detail, set 'shouldRecordBreaches' 'true' at construction");
        
        return _breaches;
    }

    class SimDetails {
        private final long _breaches;
        private final long _requests;
        private final int _id;

        SimDetails(int anId, long aTotalReqs, long aTotalBreaches) {
            _id = anId;
            _requests = aTotalReqs;
            _breaches = aTotalBreaches;
        }

        long getBreachCount() {
            return _breaches;
        }

        long getRequestCount() {
            return _requests;
        }

        public String toString() {
            return "Node: " + _id + " had " + _requests + " w/ " + _breaches + " breaches";
        }
    }

    SimDetails getSimDetails() {
        return new SimDetails(_id, _totalRequests, _totalBreaches);
    }
    
    long getBreachCount() {
        return _totalBreaches;
    }

    long getRequestCount() {
        return _totalRequests;
    }

    int currentConnections(long aCurrentTime) {
        cull(aCurrentTime);
        return _requests.size();
    }

    boolean incomingRequest(int aRequestDuration, long aCurrentTime) {
        ++_totalRequests;

        Request myReq = new Request(aRequestDuration, aCurrentTime);
        _requests.add(myReq);
        _inThrottleScope.add(myReq);

        cull(aCurrentTime);

        if (_inThrottleScope.size() > _policy.getMax()) {
            if (_recordBreaches)
                _breaches.add(new Breach(aCurrentTime, _requests.size(), _inThrottleScope.size(), _policy.getMax()));

            ++_totalBreaches;
            return true;
        }

        return false;
    }

    private void cull(long aCurrentTime) {
        Iterator<Request> myRequests = _requests.iterator();

        while (myRequests.hasNext()) {
            Request myRequest = myRequests.next();

            // List is oldest to newest so first that hasn't expired means there will be no more
            //
            if (myRequest.hasExpired(aCurrentTime))
                myRequests.remove();
            else
                break;
        }


        Request myRequest;

        while ((myRequest = _inThrottleScope.peek()) != null) {
            if (_policy.outOfScope(myRequest, aCurrentTime))
                _inThrottleScope.remove();
            else
                break;
        }
    }

    static class Breach {
        private final long _breachTime;
        private final int _queueSize;
        private final int _limit;
        private final int _throttleScope;

        Breach(long aTime, int aQueueSize, int aThrottleScope, int aLimit) {
            _breachTime = aTime;
            _queueSize = aQueueSize;
            _throttleScope = aThrottleScope;
            _limit = aLimit;
        }

        public String toString() {
            return "Breach @ " + _breachTime + " with queue size " + _queueSize +
                    " of which in scope " + _throttleScope + " against limit " + _limit;
        }
    }
}