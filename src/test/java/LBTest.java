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

    @Test(expected = IllegalArgumentException.class)
    public void testSubOneKReqsPerSecFails() {
        ThrottlePolicy myPolicy = new ThrottlePolicy(500, 1000);
        LB myLB = new LB(2, myPolicy, false);

        myLB.allocate(_reqDurations, 999);
    }
}
