package pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.collators.CalculateIndexCollator;
import pod.combiners.SpeciesCountCombinerFactory;
import pod.mappers.NeighborhoodSpeciesMapper;
import pod.models.Neighbourhood;
import pod.models.Pair;
import pod.models.Tree;
import pod.reducers.SpeciesMaxReducerFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static pod.client.Utils.parseParameter;

public class Query2 {

    private static final Logger logger = LoggerFactory.getLogger(Query2.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        // Setup logging

        File logFile = new File(parseParameter(args, "-DoutPath")+"/time2.txt");
        logFile.createNewFile();
        FileWriter logWriter = new FileWriter(logFile);

        // Create and execute job

        HazelcastInstance hz = Utils.getClientInstance(args);
        Utils.loadTreesFromCsv(args, hz, logWriter);

        final IList<Tree> trees = hz.getList("g2_trees");
        final KeyValueSource<String, Tree> dataSource = KeyValueSource.fromList(trees);

        Utils.logTimestamp(logWriter, "Inicio del trabajo map/reduce");

        JobTracker jt = hz.getJobTracker("g2_jobs");
        Job<String, Tree> job = jt.newJob(dataSource);
        ICompletableFuture<Map<String, Pair<String, Float>>> future = job
                .mapper( new NeighborhoodSpeciesMapper() )
                .combiner( new SpeciesCountCombinerFactory<>() )
                .reducer( new SpeciesMaxReducerFactory<>() )
                .submit(new CalculateIndexCollator(hz));
        Map<String, Pair<String,Float>> result = future.get();



        // Write results

        File csvFile = new File(parseParameter(args, "-DoutPath")+"/query2.csv");
        csvFile.createNewFile();
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("NEIGHBOURHOOD;COMMON_NAME;TREES_PER_PEOPLE\n");

        result.entrySet().stream()/*.sorted(
                Map.Entry.<String, String>comparingByValue().thenComparing(Map.Entry.comparingByKey()).reversed()
        )*/.forEach(e -> {
            try {
                csvWriter.write(e.getKey() + ";" + e.getValue().getLeft() + ";" + e.getValue().getRight() + "\n");
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

}
