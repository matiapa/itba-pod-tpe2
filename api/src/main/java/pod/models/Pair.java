package pod.models;

import java.io.Serializable;
import java.util.Objects;

public class Pair <K extends Comparable<K>,V extends Comparable<V>> implements Comparable<Pair<K,V>>, Serializable {
    private K left;
    private V right;

    public Pair(K left, V right) {
        this.left = left;
        this.right = right;
    }

    public K getLeft() {
        return left;
    }

    public V getRight() {
        return right;
    }


    @Override
    public int compareTo(Pair<K, V> o) {
        int rightCompares = o.right.compareTo(this.right);
        return rightCompares == 0 ? this.left.compareTo(o.left) : rightCompares;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(left, pair.left) && Objects.equals(right, pair.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }
}
