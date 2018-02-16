import org.apache.commons.math3.random.RandomGenerator;

public interface Bucket {
    int draw(RandomGenerator anRNG);

    boolean isExhausted();

    Bucket copy();
}
