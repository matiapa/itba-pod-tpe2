package pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class NeighborhoodSpeciesReducerFactory implements ReducerFactory<String,String, Integer> {
    @Override
    public Reducer<String, Integer> newReducer(String s) {
        return new NeighborhoodSpeciesReducer();
    }
    private static class NeighborhoodSpeciesReducer extends Reducer<String,Integer>{

        List<String> species= new LinkedList<>();
        @Override
        public void reduce(String s) {
            if (!species.contains(s))
                species.add(s);
        }

        @Override
        public Integer finalizeReduce() {
            return species.size()/100*100;
        }
    }
}
