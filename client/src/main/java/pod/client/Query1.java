package pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.combiners.CountCombinerFactory;
import pod.mappers.TreeByNeighMapper;
import pod.models.Tree;
import pod.reducers.CountReducerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.List;

import static pod.client.Utils.parseParameter;

public class Query1 {

    private static final Logger logger = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        // Setup logging

        File logFile = new File(parseParameter(args, "-DoutPath")+"/time1.txt");
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
        ICompletableFuture<Stream<Map.Entry<String, Long>>> future = job
            .mapper( new TreeByNeighMapper() )
            .combiner( new CountCombinerFactory<>() )
            .reducer( new CountReducerFactory<>() )
            .submit((values) ->
                StreamSupport.stream(values.spliterator(), false)
                .sorted(Map.Entry.<String, Long>comparingByValue().thenComparing(Map.Entry.comparingByKey()).reversed())
            );
        Stream<Map.Entry<String, Long>> result = future.get();

        // Write results

        File csvFile = new File(parseParameter(args, "-DoutPath")+"/query1.csv");
        csvFile.createNewFile();
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("NEIGHBOURHOOD;TREES\n");

        result.forEach(e -> {
            try {
                csvWriter.write(e.getKey() + ";" + e.getValue() + "\n");
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
