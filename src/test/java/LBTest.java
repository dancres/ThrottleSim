import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class LBTest {
    private static List<Integer> _reqDurations;

    @BeforeClass
    public static void setup() {
        _reqDurations = new LinkedList<>();

        for (int i = 0; i < 2000; i++)
            _reqDurations.add(50);
    }
    
    @Test
    public void testAllocateNoBreach() {
        ThrottlePolicy myPolicy = new ThrottlePolicy(500, 1000);
        LB myLB = new LB(2, myPolicy, false);

        myLB.allocate(_reqDurations, 1000);

        Assert.assertEquals(2, myLB.getNodes().size());
        
        for (Node myNode : myLB.getNodes()) {
            Assert.assertEquals(1000, myNode.getRequestCount());
            Assert.assertEquals(0, myNode.getBreachCount());
            Assert.assertEquals(1000, myNode.getSimDetails().getRequestCount());
            Assert.assertEquals(0, myNode.getSimDetails().getBreachCount());
        }
    }

    @Test
    public void testAllocateWithBreach() {
        ThrottlePolicy myPolicy = new ThrottlePolicy(450, 1000);
        LB myLB = new LB(2, myPolicy, false);

        myLB.allocate(_reqDurations, 1000);

        Assert.assertEquals(2, myLB.getNodes().size());

        for (Node myNode : myLB.getNodes()) {
            Assert.assertEquals(1000, myNode.getRequestCount());
            Assert.assertEquals(100, myNode.getBreachCount());
            Assert.assertEquals(1000, myNode.getSimDetails().getRequestCount());
            Assert.assertEquals(100, myNode.getSimDetails().getBreachCount());
        }
    }

    @Test
    public void testMultiMillsecondGapBetweenRequests() {
        ThrottlePolicy myPolicy = new ThrottlePolicy(200, 1000);
        LB myLB = new LB(2, myPolicy, false);

        myLB.allocate(_reqDurations, 100);

        Assert.assertEquals(2, myLB.getNodes().size());

        // At a rate of 100 requests per second (one request per 10 milliseconds) and with duration 50ms each LB
        // will allocate unevenly across nodes. One node will get 3 requests vs 2 for the other per 50 milliseconds.
        // Thus the 2 nodes get 2/5th and 3/5th of total requests respectively (800 and 1200 in total).
        //
        List<Node> myNodes = myLB.getNodes();
        Node myA = myNodes.get(0);
        Node myB = myNodes.get(1);

        // Make sure the nodes are ordered lowest first in terms of request count prior to testing outcome
        // (Different JDK implementations could produce different orders).
        //
        if (myA.getRequestCount() > myB.getRequestCount()) {
            Node myTemp = myB;
            myB = myA;
            myA = myTemp;
        }

        Assert.assertEquals(800, myA.getRequestCount());
        Assert.assertEquals(1200, myB.getRequestCount());

        // Neither node will breach given the throttle policy
        //
        Assert.assertEquals(0, myA.getBreachCount());
        Assert.assertEquals(0, myB.getBreachCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegReqsPerSecFails() {
        ThrottlePolicy myPolicy = new ThrottlePolicy(500, 1000);
        LB myLB = new LB(2, myPolicy, false);

        myLB.allocate(_reqDurations, -5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroReqsPerSecFails() {
        ThrottlePolicy myPolicy = new ThrottlePolicy(500, 1000);
        LB myLB = new LB(2, myPolicy, false);

        myLB.allocate(_reqDurations, 0);
    }
}
