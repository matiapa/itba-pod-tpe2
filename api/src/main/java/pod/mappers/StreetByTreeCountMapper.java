package pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class StreetByTreeCountMapper implements Mapper<String, Long, Long, String> {

    @Override
    public void map(String street, Long treeCount, Context<Long, String> context) {
        context.emit(treeCount, street);
    }

}
