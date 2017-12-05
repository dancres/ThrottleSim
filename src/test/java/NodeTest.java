import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NodeTest {
    private static final long SCOPE = 1000;
    private static final int LIMIT = 50;
    private static final int NODE_ID = 1;

    private Node _node;

    @Before
    public void setup() {
        ThrottlePolicy myPolicy = new ThrottlePolicy(LIMIT, SCOPE);
        _node = new Node(NODE_ID, myPolicy, false);
    }

    @Test
    public void testNodeId() {
        Assert.assertEquals(NODE_ID, _node.getId());
    }

    @Test
    public void testCurrentConnections() {
        Assert.assertEquals(0, _node.currentConnections(0));

        for (int i = 0; i < LIMIT / 2; i++) {
            _node.incomingRequest(1000, i);
        }

        Assert.assertEquals(LIMIT / 2, _node.currentConnections(0));
    }

    @Test
    public void testRequestCount() {
        Assert.assertEquals(0, _node.getRequestCount());

        for (int i = 0; i < LIMIT / 2; i++) {
            _node.incomingRequest(1000, i);
        }

        Assert.assertEquals(LIMIT / 2, _node.getRequestCount());
    }

    @Test
    public void testCollidingCurrentConnections() {
        Assert.assertEquals(0, _node.currentConnections(0));

        for (int i = 0; i < LIMIT / 2; i++) {
            // We want all connections to start and expire simultaneously so as to ensure we cope with collisions
            //
            _node.incomingRequest(1000, 0);
        }

        Assert.assertEquals(LIMIT / 2, _node.currentConnections(0));
    }
    
    @Test
    public void testCollidingRequestCount() {
        Assert.assertEquals(0, _node.getRequestCount());

        for (int i = 0; i < LIMIT / 2; i++) {
            // We want all connections to start and expire simultaneously so as to ensure we cope with collisions
            //
            _node.incomingRequest(1000, 0);
        }

        Assert.assertEquals(LIMIT / 2, _node.getRequestCount());
    }

    @Test
    public void testNoBreach() {
        for (int i = 0; i < LIMIT - 1; i++) {
            _node.incomingRequest(1000, i);
            _node.currentConnections(i);
        }

        Assert.assertEquals(LIMIT - 1, _node.getRequestCount());
        Assert.assertEquals(0, _node.getBreachCount());
    }

    @Test
    public void testSingleBreach() {
        Assert.assertEquals(0, _node.getBreachCount());
        
        for (int i = 0; i < LIMIT + 1; i++) {
            _node.incomingRequest(1000, i);
            _node.currentConnections(i);
        }

        Assert.assertEquals(LIMIT + 1, _node.getRequestCount());
        Assert.assertEquals(1, _node.getBreachCount());
    }

    @Test
    public void testMultiBreach() {
        for (int breach = 0; breach < 5; breach++)
            for (int i = 0; i < LIMIT + 1; i++) {
                // System.err.println("Request: " + breach + "," + i + "," + (i + (breach * SCOPE)));
                
                _node.incomingRequest(1000, i + (breach * SCOPE));
                _node.currentConnections(i + (breach * SCOPE));
            }

        Assert.assertEquals((LIMIT + 1) * 5, _node.getRequestCount());
        Assert.assertEquals(5, _node.getBreachCount());
    }
}
