package pod.collators;

import com.hazelcast.mapreduce.Collator;
import pod.models.StreetPair;

import java.util.*;

public class StreetPairsCollator implements Collator<Map.Entry<Long, SortedSet<String>>, SortedSet<StreetPair>> {

    @Override
    public SortedSet<StreetPair> collate(Iterable<Map.Entry<Long, SortedSet<String>>> values) {
        SortedSet<StreetPair> set = new TreeSet<>();

        values.forEach(e -> {
            long group = e.getKey();
            SortedSet<String> streets = e.getValue();

            streets.forEach(st1 -> {
                streets.forEach(st2 -> {
                    if(st1.compareTo(st2) < 0) {
                        set.add(new StreetPair(group, st1, st2));
                    }
                });
            });
        });

        return set;
    }

}
