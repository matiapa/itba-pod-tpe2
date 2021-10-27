package pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class NeighborhoodBySpeciesCountMapper implements Mapper<String, Integer, Long,String> {


    @Override
    public void map(String neighborhood, Integer speciesCount, Context<Long, String> context) {
        if (speciesCount>99)
            context.emit((long)speciesCount/100*100,neighborhood);
    }

}
