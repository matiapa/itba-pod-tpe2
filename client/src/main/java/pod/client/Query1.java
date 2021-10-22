package pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.models.Tree;
import pod.query1.TreeCountMapper;
import pod.query1.TreeCountReducerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query1 {

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
        ICompletableFuture<Map<String, Long>> future = job
            .mapper( new TreeCountMapper() )
            .reducer( new TreeCountReducerFactory() )
            .submit();
        Map<String, Long> result = future.get();

        Utils.logTimestamp(logWriter, "Fin del trabajo map/reduce");
        logWriter.close();

        // Write results

        File csvFile = new File("query1.txt");
        csvFile.createNewFile();
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("NEIGHBOURHOOD;TREES\n");
        result.entrySet().stream().sorted(
            Map.Entry.comparingByValue()
        ).forEach(e -> {
            try {
                csvWriter.write(e.getKey() + ";" + e.getValue() + "\n");
            } catch (IOException err) {
                err.printStackTrace();
            }
        });
        csvWriter.close();

        System.out.println(result);
    }

}
