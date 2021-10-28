package pod.collators;

import com.hazelcast.mapreduce.Collator;
import pod.models.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SetSizeCollatorWithOrder implements Collator<Map.Entry<String, Set<String>>, Stream<Pair<String,Integer>>> {

    public SetSizeCollatorWithOrder(int n) {
        this.n = n;
    }

    private int n;

    @Override
    public Stream<Pair<String,Integer>> collate(Iterable<Map.Entry<String, Set<String>>> iterable) {

        SortedSet<Pair<String,Integer>> result = new TreeSet<>();

        


        for (Map.Entry<String,Set<String>> item : iterable){

            result.add(new Pair<>(item.getKey(),item.getValue().size()));
        }

        return result.stream().limit(n);

    }
}
