package pod.collators;

import com.hazelcast.mapreduce.Collator;
import pod.models.Pair;

import java.util.*;


public class SetSizeCollatorWithOrder implements Collator<Map.Entry<String, Set<String>>, SortedSet<Pair<String,Integer>>> {
    @Override
    public SortedSet<Pair<String,Integer>> collate(Iterable<Map.Entry<String, Set<String>>> iterable) {

        SortedSet<Pair<String,Integer>> result = new TreeSet<>();

        


        for (Map.Entry<String,Set<String>> item : iterable){
            result.add(new Pair<>(item.getKey(),item.getValue().size()));
        }

        return result;


    }
}
