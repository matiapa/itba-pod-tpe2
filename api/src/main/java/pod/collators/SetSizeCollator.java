package pod.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.*;


public class SetSizeCollator implements Collator<Map.Entry<String, Set<String>>, Map<String,Integer>> {
    @Override
    public Map<String,Integer> collate(Iterable<Map.Entry<String, Set<String>>> iterable) {

        Map<String,Integer> result = new HashMap<>();

        for (Map.Entry<String,Set<String>> item : iterable){
            result.put(item.getKey(),item.getValue().size());
        }

        return result;


    }
}
