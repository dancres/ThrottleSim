import sample.BucketConsumer;

import java.util.ArrayList;
import java.util.List;

class LB {
    private final List<Node> _nodes = new ArrayList<>();
    private final boolean _debug;

    LB(int aNumNodes, ThrottlePolicy aPolicy, boolean isDebug) {
        for (int i = 0; i < aNumNodes; i++)
            _nodes.add(new Node(i, aPolicy, isDebug));

        _debug = isDebug;
    }

    /**
     * @param aReqsPerSec must be > 0
     */
    void allocate(BucketConsumer<Integer> aConsumer, int aReqsPerSec) {
        if (aReqsPerSec <= 0)
            throw new IllegalArgumentException("Requests per Second must be > 0");
        
        double myMillisPerReq = 1000.0 / aReqsPerSec;
        long myCurrentTick = 0; // In seconds
        int myReqCount = 0;

        // Scatter the requests evenly across the seconds of runtime millisecond by millisecond
        //
        while (aConsumer.claim()) {
            Integer myDuration = aConsumer.nextSample();

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
        int myFavouriteConnectionCount = Integer.MAX_VALUE;
        Node myFavouriteNode = null;

        for (Node myNode: _nodes) {
            int myConnectionCount = myNode.currentConnections(aCurrentTime);

            if (myConnectionCount < myFavouriteConnectionCount) {
                myFavouriteConnectionCount = myConnectionCount;
                myFavouriteNode = myNode;
            }
        }

        return myFavouriteNode;
    }
}

