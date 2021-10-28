package pod.collators;

import com.hazelcast.mapreduce.Collator;
import pod.models.NeighborPairs;

import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;


public class SetSortCollator implements Collator<Map.Entry<Long, SortedSet<String>>, List<NeighborPairs>> {

    @Override
    public List<NeighborPairs> collate(Iterable<Map.Entry<Long, SortedSet<String>>> iterable) {


        List<NeighborPairs>list=new LinkedList<>();
        StreamSupport.stream(iterable.spliterator(),false).sorted(
                Map.Entry.<Long, SortedSet<String>>comparingByKey().thenComparing(e -> e.getValue().first()).reversed()
        ).forEach(e -> {
            // Write each unique street pair combination
            e.getValue().forEach(st1 -> {
                e.getValue().forEach(st2 -> {
                    if(st1.compareTo(st2) < 0) {
                        list.add(new NeighborPairs(e.getKey(),st1,st2));
                    }
                });
            });
        });


        return list;
    }
}
