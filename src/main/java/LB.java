import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

class LB {
    private final List<Node> _nodes = new ArrayList<>();
    private final int _reqsPerSec;
    private final double _reqsPerMillis;
    private final boolean _debug;

    LB(int aNumNodes, ThrottlePolicy aPolicy, int aReqsPerSec, boolean isDebug) {
        for (int i = 0; i < aNumNodes; i++)
            _nodes.add(new Node(i, aPolicy, isDebug));

        _reqsPerSec = aReqsPerSec;
        _reqsPerMillis = _reqsPerSec / 1000;
        _debug = isDebug;
    }

    void allocate(List<Integer> aRequestDurations) {
        long myCurrentTick = 0; // In seconds
        int myReqCount = 0;

        // Scatter the requests evenly across the seconds of runtime millisecond by millisecond
        //
        for (Integer myDuration : aRequestDurations) {

            // Current time is a second + the request index for that second / the reqs per milli - allocate a request down to milliseconds
            //
            long myCurrentTime = (long) Math.floor((myCurrentTick * 1000) + (myReqCount / _reqsPerMillis));

            boolean myOutcome = findTargetNode(myCurrentTime).incomingRequest(myDuration, myCurrentTime);

            if (_debug)
                if (myOutcome)
                    System.out.print("B");
                else
                    System.out.print(".");

            ++myReqCount;

            // If we're done with requests for this second, start on the next
            //
            if (myReqCount == _reqsPerSec) {
                ++myCurrentTick;
                myReqCount = 0;
            }
        }
    }

    List<Node> getNodes() {
        return _nodes;
    }

    private Node findTargetNode(long aCurrentTime) {
        SortedMap<Integer, Node> myNodes = new TreeMap<>();

        for (Node myNode : _nodes)
            myNodes.put(myNode.currentConnections(aCurrentTime), myNode);

        return myNodes.get(myNodes.firstKey());
    }
}

