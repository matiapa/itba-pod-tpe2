package pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.Map;


public class SpeciesMaxReducerFactory<K, T extends Comparable<T>> implements ReducerFactory<K, Map<T, Integer>, T> {
    @Override
    public Reducer<Map<T, Integer>, T> newReducer(K key) {
        return new SpeciesMaxReducer<>();
    }

    private static class SpeciesMaxReducer<T extends Comparable<T>> extends Reducer<Map<T, Integer>, T> {
        private Map<T, Integer> hash;

        @Override
        public void beginReduce() {
            hash = new HashMap<>();
        }

        @Override
        public void reduce(Map<T, Integer> value) {
            Integer count;
            for (Map.Entry<T, Integer> entry : value.entrySet()) {
                count = hash.get(entry.getKey());
                if (count == null)
                    hash.put(entry.getKey(), entry.getValue());
                else
                    hash.put(entry.getKey(), count + entry.getValue());
            }
        }

        @Override
        public T finalizeReduce() {
            return hash.entrySet().stream()
                    .sorted(Map.Entry.<T, Integer>comparingByValue().reversed())
                    .findFirst().get().getKey();
        }
    }
}