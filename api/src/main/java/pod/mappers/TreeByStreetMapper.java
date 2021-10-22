package pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import pod.models.Tree;

public class TreeByStreetMapper implements Mapper<String, Tree, String, Long> {

    private static final Long ONE = 1L;

    @Override
    public void map(String key, Tree value, Context<String, Long> context) {
        context.emit(value.getStreet(), ONE);
    }

}
