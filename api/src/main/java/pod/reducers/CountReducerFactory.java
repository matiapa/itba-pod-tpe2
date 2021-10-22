package pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CountReducerFactory<K> implements ReducerFactory<K, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(K key) {
        return new TreeCountReducer();
    }

    private static class TreeCountReducer extends Reducer<Long, Long> {
        private volatile long sum;

        @Override
        public void beginReduce() {
            sum = 0;
        }

        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }

}