import org.apache.commons.math3.random.RandomGenerator;

public interface Bucket<T> {
    T draw(RandomGenerator anRNG);

    int numRemaining();

    Bucket<T> copy();
}
