package pod.mappers;

import com.hazelcast.mapreduce.Context;
import pod.models.Tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NeighbourhoodSpeciesMapper implements com.hazelcast.mapreduce.Mapper<String, Tree,String,Long> {

    private static final Long ONE = 1L;
    Map<String, List<String>> memory = new HashMap<>();


    @Override
    public void map(String s, Tree tree, Context<String, Long> context) {
        if(memory.get(tree.getNeighbour()) == null){
            List<String> newList = new ArrayList<>();
            newList.add(tree.getName());
            memory.put(tree.getNeighbour(),newList);
            context.emit(tree.getNeighbour(),ONE);
        }
        else if (!memory.get(tree.getNeighbour()).contains(tree.getName())){
            memory.get(tree.getNeighbour()).add(tree.getName());
            context.emit(tree.getNeighbour(),ONE);
        }
    }
}
