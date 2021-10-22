package pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.*;

public class SortedSetCombinerFactory<K,T extends Comparable<T>> implements CombinerFactory<K, T, SortedSet<T>> {

    @Override
    public Combiner<T, SortedSet<T>> newCombiner(K key ) {
        return new SortedSetCombiner<>();
    }

    static class SortedSetCombiner<T extends Comparable<T>> extends Combiner<T, SortedSet<T>> {
        private SortedSet<T> list = new TreeSet<>();

        @Override
        public void combine(T value) {
            list.add(value);
        }

        @Override
        public SortedSet<T> finalizeChunk() {
            return list;
        }

        @Override
        public void reset() {
            list = new TreeSet<>();
        }
    }

}