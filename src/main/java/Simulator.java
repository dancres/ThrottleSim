import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

class Simulator implements Callable<Simulator> {
    private final boolean _debug;
    private final DurationProducer _producer;
    private final LB _loadBalancer;
    private final int _reqsPerSec;

    private long _requestTotal = 0;
    private long _breachTotal = 0;
    private int _breachedNodeCount = 0;

    private Map<Integer, List<Node.Breach>> _breachDetail = new HashMap<>();

    Simulator(boolean isDebug, DurationProducer aProducer, int aReqsPerSec, LB aBalancer) {
        _debug = isDebug;
        _producer = aProducer;
        _reqsPerSec = aReqsPerSec;
        _loadBalancer = aBalancer;
    }

    @Override
    public Simulator call() {
        _loadBalancer.allocate(_producer.produce(), _reqsPerSec);

        for (Node myNode : _loadBalancer.getNodes()) {
            long myBreaches = myNode.getBreachCount();

            _requestTotal += myNode.getRequestCount();

            if (myBreaches != 0) {
                _breachTotal += myBreaches;
                _breachedNodeCount++;
            }
        }

        if (_debug) {
            for (Node myNode : _loadBalancer.getNodes()) {
                _breachDetail.put(myNode.getId(), myNode.getBreaches());
            }
        }

        return this;
    }

    Map<Integer, List<Node.Breach>> getBreachDetail() { return _breachDetail; }

    long getRequestTotal() { return _requestTotal; }

    long getBreachTotal() { return _breachTotal; }

    int getBreachedNodeTotal() { return _breachedNodeCount; }

    List<Node.SimDetails> simDetailsByNode() {
        List<Node.SimDetails> myDetails = new LinkedList<>();

        for (Node myNode: _loadBalancer.getNodes()) {
            myDetails.add(myNode.getSimDetails());
        }

        return myDetails;
    }
}

