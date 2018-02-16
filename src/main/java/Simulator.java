import org.apache.commons.math3.random.RandomGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

class Simulator implements Callable<Simulator> {
    private final boolean _debug;
    private final LB _loadBalancer;
    private final int _reqsPerSec;
    private final RandomGenerator _rng;
    private final BucketConsumer _consumer;
    private long _requestTotal = 0;
    private long _breachTotal = 0;
    private int _breachedNodeCount = 0;

    private Map<Integer, List<Node.Breach>> _breachDetail = new HashMap<>();

    Simulator(boolean isDebug, Bucket[] aBuckets, int aReqsPerSec, LB aBalancer,
              RandomGenerator aGen) {
        _debug = isDebug;
        _consumer = new BucketConsumer(aBuckets, aGen);
        _reqsPerSec = aReqsPerSec;
        _loadBalancer = aBalancer;
        _rng = aGen;
    }

    @Override
    public Simulator call() {
        _loadBalancer.allocate(_consumer, _reqsPerSec);

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
        return _loadBalancer.getNodes().stream().map(Node::getSimDetails).collect(Collectors.toList());
    }
}

