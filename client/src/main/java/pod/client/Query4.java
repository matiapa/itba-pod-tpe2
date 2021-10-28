package pod.client;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.collators.SetSizeCollator;
import pod.collators.SetSizeCollatorWithOrder;
import pod.collators.SetSortCollator;
import pod.combiners.SetCombinerFactory;
import pod.combiners.SortedSetCombinerFactory;
import pod.mappers.NeighborhoodBySpeciesCountMapper;
import pod.mappers.NeighborhoodSpeciesMapper;
import pod.models.NeighborPairs;
import pod.models.Pair;
import pod.models.Tree;
import pod.reducers.SetReducerFactory;
import pod.reducers.SortedSetReducerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static pod.client.Utils.parseParameter;

public class Query4 {

    private static final Logger logger = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        // Setup logging

        File logFile = new File("time1.txt");
        logFile.createNewFile();
        FileWriter logWriter = new FileWriter(logFile);

        // Create and execute job


        HazelcastInstance hz = Utils.getClientInstance(args);
        Utils.loadTreesFromCsv(args, hz, logWriter);

        Utils.logTimestamp(logWriter, "Inicio del trabajo map/reduce");


        final IList<Tree> trees = hz.getList("g2_trees");

        List <NeighborPairs> result = getMapReduceResult(hz,trees);


        // Write results

        File csvFile = new File(parseParameter(args, "-DoutPath") + "/query4.csv");
        csvFile.createNewFile();
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("GROUP;NEIGHBOURHOOD A;NEIGHBOURHOOD B\n");
//        if (parseParameter(args, "-Dcity").equals("BUE")){
//            for (Long key:result2.keySet()) {
//                SortedSet<String> aux=new TreeSet<>(Comparator.comparingInt(Integer::parseInt));
//                aux.addAll(result2.get(key));
//                result2.put(key,aux);
//            }
//        }

        result.forEach(neighborPairs -> {
            try {
                csvWriter.write(neighborPairs.getGroup() + ";" + neighborPairs.getNeighborhoodA() + ";" + neighborPairs.getNeighborhoodB() + "\n");
            } catch (IOException err) {
                logger.error(err.getMessage());
                HazelcastClient.shutdownAll();

            }
        });



        Utils.logTimestamp(logWriter, "Fin del trabajo map/reduce");
        logWriter.close();

        csvWriter.close();

        HazelcastClient.shutdownAll();
    }


    public static List<NeighborPairs> getMapReduceResult(HazelcastInstance hz, IList<Tree> trees) throws ExecutionException, InterruptedException {

        final KeyValueSource<String, Tree> dataSource = KeyValueSource.fromList(trees);


        JobTracker jt = hz.getJobTracker("g2_jobs");
        Job<String, Tree> job = jt.newJob(dataSource);
        ICompletableFuture<Map<String,Integer>> future = job
                .mapper( new NeighborhoodSpeciesMapper())
                .combiner(new SetCombinerFactory<>())
                .reducer( new SetReducerFactory<>())
                .submit(new SetSizeCollator());
        Map<String,Integer> result =future.get();

        // Transform previous map to Map: Amount of trees -> SortedSet<Street>

        final IMap<String, Integer> treeSpeciesCountByNeighborhood = hz.getMap("g2_treeSpeciesCountByNeighborhood");
        treeSpeciesCountByNeighborhood.clear();
        treeSpeciesCountByNeighborhood.putAll(result);

        KeyValueSource<String, Integer> dataSource2 = KeyValueSource.fromMap(treeSpeciesCountByNeighborhood);

        Job<String, Integer> job2 = jt.newJob(dataSource2);
        ICompletableFuture<List<NeighborPairs>> future2 = job2
                .mapper(new NeighborhoodBySpeciesCountMapper())
                .combiner(new SortedSetCombinerFactory<>())
                .reducer(new SortedSetReducerFactory<>())
                .submit(new SetSortCollator());
        return future2.get();
    }
}
