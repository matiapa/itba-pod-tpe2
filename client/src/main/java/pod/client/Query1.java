package pod.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.models.Tree;
import pod.combiners.CountCombinerFactory;
import pod.mappers.TreeByNeighMapper;
import pod.reducers.CountReducerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

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
        ICompletableFuture<Map<String, Long>> future = job
            .mapper( new TreeByNeighMapper() )
            .combiner( new CountCombinerFactory<>() )
            .reducer( new CountReducerFactory<>() )
            .submit();
        Map<String, Long> result = future.get();

        Utils.logTimestamp(logWriter, "Fin del trabajo map/reduce");
        logWriter.close();

        // Write results

        File csvFile = new File(parseParameter(args, "-DoutPath")+"/query1.txt");
        csvFile.createNewFile();
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("NEIGHBOURHOOD;TREES\n");

        result.entrySet().stream().sorted(
            Map.Entry.<String, Long>comparingByValue().thenComparing(Map.Entry.comparingByKey()).reversed()
        ).forEach(e -> {
            try {
                csvWriter.write(e.getKey() + ";" + e.getValue() + "\n");
            } catch (IOException err) {
                err.printStackTrace();
            }
        });
        csvWriter.close();
    }

}
