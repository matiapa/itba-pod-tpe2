package pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CountCombinerFactory<K> implements CombinerFactory<K, Long, Long> {

    @Override
    public Combiner<Long, Long> newCombiner(K key ) {
        return new TreeCountCombiner();
    }

    static class TreeCountCombiner extends Combiner<Long, Long> {
        private long sum = 0;

        @Override
        public void combine(Long value) {
            sum++;
        }

        @Override
        public Long finalizeChunk() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }

}