package pod.reducers;

import com.hazelcast.mapreduce.Reducer;

import java.util.*;

import com.hazelcast.mapreduce.ReducerFactory;

public class SortedSetReducerFactory<K, T extends Comparable<T>> implements ReducerFactory<K, SortedSet<T>, SortedSet<T>> {

    @Override
    public Reducer<SortedSet<T>, SortedSet<T>> newReducer(K key) {
        return new SortedSetReducer<>();
    }

    private static class SortedSetReducer<T extends Comparable<T>> extends Reducer<SortedSet<T>, SortedSet<T>> {
        private volatile SortedSet<T> streets;

        @Override
        public void beginReduce() {
            streets = new TreeSet<>();
        }

        @Override
        public void reduce(SortedSet<T> value) {
            streets.addAll(value);
        }

        @Override
        public SortedSet<T> finalizeReduce() {
            return streets;
        }
    }

}