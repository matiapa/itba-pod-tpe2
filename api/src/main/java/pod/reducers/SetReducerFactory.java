package pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Set;
import java.util.TreeSet;

public class SetReducerFactory<K, T extends Comparable<T>> implements ReducerFactory<K, Set<T>, Set<T>> {

    @Override
    public Reducer<Set<T>, Set<T>> newReducer(K key) {
        return new SetReducer<>();
    }

    private static class SetReducer<T extends Comparable<T>> extends Reducer<Set<T>, Set<T>> {
        private volatile Set<T> streets;

        @Override
        public void beginReduce() {
            streets = new TreeSet<>();
        }

        @Override
        public void reduce(Set<T> value) {
            streets.addAll(value);
        }

        @Override
        public Set<T> finalizeReduce() {
            return streets;
        }
    }
}