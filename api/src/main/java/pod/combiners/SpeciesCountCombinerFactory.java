package pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashMap;
import java.util.Map;

/*import java.util.Set;
import java.util.TreeSet;*/

public class SpeciesCountCombinerFactory<K,T extends Comparable<T>> implements CombinerFactory<K, T, Map<T, Integer>> {

    @Override
    public Combiner<T, Map<T, Integer>> newCombiner(K key ) {
        return new SpeciesCountCombiner<>();
    }

    static class SpeciesCountCombiner<T extends Comparable<T>> extends Combiner<T, Map<T, Integer>> {
        private Map<T, Integer> hash = new HashMap<>();

        @Override
        public void combine(T value) {
            Integer count = hash.get(value);
            if (count == null)
                hash.put(value, 1);
            else
                hash.put(value, count + 1);
        }

        @Override
        public Map<T, Integer> finalizeChunk() {
            return hash;
        }

        @Override
        public void reset() {
            hash = new HashMap<>();
        }
    }

}