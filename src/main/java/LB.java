import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

class LB {
    private final List<Node> _nodes = new ArrayList<>();
    private final boolean _debug;

    LB(int aNumNodes, ThrottlePolicy aPolicy, boolean isDebug) {
        for (int i = 0; i < aNumNodes; i++)
            _nodes.add(new Node(i, aPolicy, isDebug));

        _debug = isDebug;
    }

    /**
     *
     * @param aRequestDurations
     * @param aReqsPerSec must be > 0
     */
    void allocate(List<Integer> aRequestDurations, int aReqsPerSec) {
        if (aReqsPerSec <= 0)
            throw new IllegalArgumentException("Requests per Second must be > 0");
        
        double myMillisPerReq = 1000 / aReqsPerSec;
        long myCurrentTick = 0; // In seconds
        int myReqCount = 0;

        // Scatter the requests evenly across the seconds of runtime millisecond by millisecond
        //
        for (Integer myDuration : aRequestDurations) {

            // Current time is a second + the request index for that second / the reqs per milli -
            // allocate a request down to milliseconds
            //
            // long myCurrentTime = (long) Math.floor((myCurrentTick * 1000) + (myReqCount * myMillisPerReq));
            long myCurrentTime = Math.round((myCurrentTick * 1000) + (myReqCount * myMillisPerReq));
            boolean myOutcome = findTargetNode(myCurrentTime).incomingRequest(myDuration, myCurrentTime);

            if (_debug)
                if (myOutcome)
                    System.out.print("B");
                else
                    System.out.print(".");

            ++myReqCount;

            // If we're done with requests for this second, start on the next
            //
            if (myReqCount == aReqsPerSec) {
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

