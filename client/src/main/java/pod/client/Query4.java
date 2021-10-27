package pod.client;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.collators.NeighborhoodSpeciesCollator;
import pod.mappers.NeighborhoodSpeciesMapper;
import pod.models.NeighborPairs;
import pod.models.Tree;
import pod.reducers.NeighborhoodSpeciesReducerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

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

        final IList<Tree> trees = hz.getList("g2_trees");
        final KeyValueSource<String, Tree> dataSource = KeyValueSource.fromList(trees);

        Utils.logTimestamp(logWriter, "Inicio del trabajo map/reduce");

        JobTracker jt = hz.getJobTracker("g2_jobs");
        Job<String, Tree> job = jt.newJob(dataSource);
        ICompletableFuture<List<NeighborPairs>> future = job
                .mapper( new NeighborhoodSpeciesMapper())
//                .combiner( new CountCombinerFactory<>() )
                .reducer( new NeighborhoodSpeciesReducerFactory())
                .submit(new NeighborhoodSpeciesCollator());
        List<NeighborPairs> result = future.get();

        Utils.logTimestamp(logWriter, "Fin del trabajo map/reduce");
        logWriter.close();

        // Write results

        File csvFile = new File("query4.txt");
        csvFile.createNewFile();
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("GROUP;NEIGHBOURHOOD A;NEIGHBOURHOOD B\n");

        result.stream().sorted().forEach(e -> {
            try {
                csvWriter.write(e.getGroup() + ";" + e.getNeighborhoodA()  + ";" + e.getNeighborhoodB() + "\n");
            } catch (IOException err) {
                err.printStackTrace();
            }
        });
        csvWriter.close();
    }

}
