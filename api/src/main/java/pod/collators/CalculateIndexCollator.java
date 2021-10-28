package pod.collators;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;
import pod.models.Neighbourhood;
import pod.models.Pair;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class CalculateIndexCollator implements Collator<Map.Entry<String, Pair<String, Integer>>, Map<String, Pair<String, Float>>> {
    HazelcastInstance hz;
    public CalculateIndexCollator(HazelcastInstance hz) {
        super();
        this.hz = hz;
    }

    @Override
    public Map<String,Pair<String,Float>> collate(Iterable<Map.Entry<String, Pair<String, Integer>>> iterable) {

        Map<String, Pair<String, Float>> hash = new HashMap<>();
        Float index;
        IMap<String, Neighbourhood> neighbourhoodIMap = hz.getMap("g2_neighbourhoods");
        for (Map.Entry<String,Pair<String, Integer>> item : iterable) {
            index = Float.valueOf(item.getValue().getRight()) / Float.valueOf(neighbourhoodIMap.get(item.getKey()).getPopulation());
            hash.put(item.getKey(), new Pair<>(item.getValue().getLeft(), (float)Math.floor(index * 100) / 100));
        }

        Map<String, Pair<String, Float>> sortedHash = new LinkedHashMap<>();
        hash.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> sortedHash.put(entry.getKey(), entry.getValue()));

        return sortedHash;




    }
}
