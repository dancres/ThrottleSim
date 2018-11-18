import org.apache.commons.math3.random.Well44497b;
import org.junit.Assert;
import org.junit.Test;
import sample.Bucket;
import sample.BucketConsumer;

public class BucketConsumerTest {
    @Test
    public void test() {
        @SuppressWarnings("unchecked")
        Bucket<Integer>[] myBuckets = new Bucket[2];
        myBuckets[0] = new FixedDurationBucket(50, 2000);
        myBuckets[1] = new FixedDurationBucket(100, 2000);

        BucketConsumer<Integer> myConsumer = new BucketConsumer<>(myBuckets, new Well44497b());

        int myOneHundreds = 0;
        int myFifties = 0;

        while (myConsumer.claim()) {
            int myOutput = myConsumer.nextSample();

            switch (myOutput) {
                case 50 : ++myFifties ; break;
                case 100 : ++myOneHundreds ; break;
                default : throw new IllegalStateException();
            }
        }

        Assert.assertEquals(4000, myFifties + myOneHundreds);
    }

    @Test
    public void timeCeilingTest() {
        @SuppressWarnings("unchecked")
        Bucket<Integer>[] myBuckets = new Bucket[1];
        myBuckets[0] = new TimeCeilingBucket(100, 100.0, 2000);
        BucketConsumer<Integer> myConsumer = new BucketConsumer<>(myBuckets, new Well44497b());

        int myTotal = 0;

        while (myConsumer.claim()) {
            myConsumer.nextSample();
            myTotal++;
        }

        Assert.assertEquals(2000, myTotal);
    }
}
