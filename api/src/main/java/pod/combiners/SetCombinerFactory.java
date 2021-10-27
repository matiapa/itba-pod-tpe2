package pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.Set;
import java.util.TreeSet;

public class SetCombinerFactory<K,T extends Comparable<T>> implements CombinerFactory<K, T, Set<T>> {

    @Override
    public Combiner<T, Set<T>> newCombiner(K key ) {
        return new SetCombiner<>();
    }

    static class SetCombiner<T extends Comparable<T>> extends Combiner<T, Set<T>> {
        private Set<T> list = new TreeSet<>();

        @Override
        public void combine(T value) {
            list.add(value);
        }

        @Override
        public Set<T> finalizeChunk() {
            return list;
        }

        @Override
        public void reset() {
            list = new TreeSet<>();
        }
    }

}