package pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import pod.models.Tree;
import pod.models.TreeEntry;

public class NeighborhoodSpeciesMapper implements Mapper<String, Tree, String,String> {
    // recibo row number, tree entity y devuelvo neighborhood, species
    @Override
    public void map(String string, Tree tree, Context<String, String> context) {
        context.emit(tree.getNeighbour(),tree.getName());
    }


//    @Override
//    public void map(Integer integer, TreeEntry treeEntry, Context<String, Integer> context) {
//        context.emit(treeEntry.getNeighborhood()+";"+treeEntry.getSpecies(),0);
//    }

}
