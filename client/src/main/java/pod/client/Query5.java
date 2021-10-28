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
import pod.combiners.SortedSetCombinerFactory;
import pod.models.Tree;
import pod.combiners.CountCombinerFactory;
import pod.reducers.CountReducerFactory;
import pod.mappers.StreetByTreeCountMapper;
import pod.mappers.TreeByStreetMapper;
import pod.reducers.SortedSetReducerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static pod.client.Utils.parseParameter;

public class Query5 {

    private static final Logger logger = LoggerFactory.getLogger(Query5.class);

    // -Dcity=BUE -Daddresses=127.0.0.1 -DinPath=. -DoutPath=. -Dneighbourhood=7 -DcommonName="Fraxinus pennsylvanica"
    // -Dcity=VAN -Daddresses=127.0.0.1 -DinPath=. -DoutPath=. -Dneighbourhood=KITSILANO -DcommonName=NORWAY_MAPLE

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        // Setup logging and client

        File logFile = new File(parseParameter(args, "-DoutPath")+"/time5.txt");
        logFile.createNewFile();
        FileWriter logWriter = new FileWriter(logFile);

        HazelcastInstance hz = Utils.getClientInstance(args);
        JobTracker jt = hz.getJobTracker("g2_jobs");

        String neighbourhood = parseParameter(args, "-Dneighbourhood").replace("_"," ");
        String commonName = parseParameter(args, "-DcommonName").replace("_"," ");

        Utils.loadTreesFromCsv(args, hz, logWriter, (t -> t.getNeighbour().equals(neighbourhood)
                && t.getName().equals(commonName)));

        // Transform tree list to Map: Street -> Amount of trees

        final IList<Tree> trees = hz.getList("g2_trees");
        KeyValueSource<String, Tree> dataSource = KeyValueSource.fromList(trees);

        Utils.logTimestamp(logWriter, "Inicio del trabajo map/reduce");

        Job<String, Tree> job = jt.newJob(dataSource);
        ICompletableFuture<Map<String, Long>> future = job
            .mapper( new TreeByStreetMapper() )
            .combiner( new CountCombinerFactory<>() )
            .reducer( new CountReducerFactory<>() )
            .submit();
        Map<String, Long> result = future.get();

        // Transform previous map to Map: Amount of trees -> SortedSet<Street>

        final IMap<String, Long> treeCountByStreet = hz.getMap("g2_treeCountByStreet");
        treeCountByStreet.clear();
        result.forEach(treeCountByStreet::put);

        KeyValueSource<String, Long> dataSource2 = KeyValueSource.fromMap(treeCountByStreet);

        Job<String, Long> job2 = jt.newJob(dataSource2);
        ICompletableFuture<Map<Long, SortedSet<String>>> future2 = job2
            .mapper( new StreetByTreeCountMapper() )
            .combiner( new SortedSetCombinerFactory<>() )
            .reducer( new SortedSetReducerFactory<>() )
            .submit();
        Map<Long, SortedSet<String>> result2 = future2.get();


        Utils.logTimestamp(logWriter, "Fin del trabajo map/reduce");
        logWriter.close();

        // Write results

        File csvFile = new File(parseParameter(args, "-DoutPath")+"/query5.txt");
        csvFile.createNewFile();
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("GROUP;STREET A; STREET B\n");

        result2.entrySet().stream().sorted(
            Map.Entry.<Long, SortedSet<String>>comparingByKey().thenComparing(e -> e.getValue().first()).reversed()
        ).forEach(e -> {
            // Write each unique street pair combination
            e.getValue().forEach(st1 -> {
                e.getValue().forEach(st2 -> {
                    if(st1.compareTo(st2) < 0) {
                        try {
                            csvWriter.write(e.getKey() + ";" + st1 + ";" + st2 + "\n");
                        } catch (IOException err) {
                            err.printStackTrace();
                        }
                    }
                });
            });
        });
        csvWriter.close();
        HazelcastClient.shutdownAll();

    }

}
