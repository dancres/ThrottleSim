import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThrottlePolicyTest {
    private static final int LIMIT = 50;
    private static final int SCOPE = 1000;

    private static ThrottlePolicy _policy;

    @BeforeClass
    public static void setup() {
        _policy = new ThrottlePolicy(LIMIT, SCOPE);
    }

    @Test
    public void testConstructor() {
        Assert.assertEquals(LIMIT, _policy.getMax());
        Assert.assertEquals(SCOPE, _policy.getScopeMillis());
    }

    @Test
    public void testInScope() {
        Request myReq = new Request(SCOPE, 0);

        Assert.assertFalse(_policy.outOfScope(myReq, 0));
        Assert.assertFalse(_policy.outOfScope(myReq, SCOPE / 2));

        myReq = new Request(SCOPE, SCOPE + 1);

        Assert.assertFalse(_policy.outOfScope(myReq, 0));
        Assert.assertFalse(_policy.outOfScope(myReq, SCOPE / 2));
    }

    @Test
    public void testOutOfScope() {
        Request myReq = new Request(SCOPE, 0);

        Assert.assertTrue(_policy.outOfScope(myReq, SCOPE + 1));

        myReq = new Request(SCOPE, SCOPE + 1);

        Assert.assertFalse(_policy.outOfScope(myReq, SCOPE + 1));
        Assert.assertTrue(_policy.outOfScope(myReq, (SCOPE * 2) + 1));
    }
}
