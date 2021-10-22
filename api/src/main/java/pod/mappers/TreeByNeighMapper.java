package pod.mappers;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import pod.models.Tree;

import java.util.List;

public class TreeByNeighMapper implements Mapper<String, Tree, String, Long> {

    private static final Long ONE = 1L;

    @Override
    public void map(String key, Tree value, Context<String, Long> context) {
        context.emit(value.getNeighbour(), ONE);
    }

}
