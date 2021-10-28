package pod.collators;

import com.hazelcast.mapreduce.Collator;
import pod.models.NeighborPairs;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class NeighborhoodSpeciesCollator implements Collator<Map.Entry<String,Integer>,List<NeighborPairs>>{
    @Override
    public List<NeighborPairs> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        Map<Integer, List<String>> preresult= new HashMap<>();
        for (Map.Entry<String, Integer> entry: iterable  ) { //da vuelta clave ,valor-> valor,clave
            if (entry.getValue()==0){//filtro los casos del grupo 0
                continue;
            }
            if (preresult.containsKey(entry.getValue()))
                preresult.get(entry.getValue()).add(entry.getKey());
            else {
                List<String>list=new LinkedList<>();
                list.add(entry.getKey());
                preresult.put(entry.getValue(), list);
            }
        }
        List<NeighborPairs> result= new LinkedList<>();
        for (Integer key: preresult.keySet()) {
            while (preresult.get(key).size()>1){
                List<String>list=preresult.get(key);

                for (int i = 1; i < list.size() ; i++) {
                    result.add(new NeighborPairs((long)key, list.get(0),list.get(i) ));
                }

            }
        }
        return result;
    }

}
