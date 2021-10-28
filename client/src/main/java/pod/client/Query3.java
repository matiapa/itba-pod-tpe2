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
import pod.collators.SetSizeCollator;
import pod.combiners.SetCombinerFactory;
import pod.mappers.NeighborhoodSpeciesMapper;
import pod.models.Tree;
import pod.reducers.SetReducerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static pod.client.Utils.parseParameter;

public class Query3 {

    private final static Logger logger = LoggerFactory.getLogger(Query3.class);


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        String outPath = parseParameter(args,"-DoutPath");


        File logFile = new File(outPath+"/time3.txt");
        System.out.println(logFile.createNewFile());
        FileWriter logWriter = new FileWriter(logFile);


        HazelcastInstance hazelcastInstance = Utils.getClientInstance(args);

        Utils.loadTreesFromCsv(args,hazelcastInstance,logWriter);





        Utils.logTimestamp(logWriter, "Inicio del trabajo map/reduce");

        IList<Tree> trees = hazelcastInstance.getList("g2_trees");


        Map<String,Integer> result = getMapReduceResult(hazelcastInstance,trees);

        AtomicInteger n = new AtomicInteger();
        try {
            n.set(Integer.parseInt(parseParameter(args, "-Dn")));

            if (n.get() < 0)
                logger.error("<n> param must be an integer greater than zero");

        }catch (NumberFormatException e){
            logger.error("<n> param must be an integer greater than zero");

        }


        Utils.logTimestamp(logWriter, "Fin del trabajo map/reduce");

        logWriter.close();
        // Write results

        File csvFile = new File(outPath+"/query3.txt");
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("NEIGHBOURHOOD;COMMON_NAME_COUNT\n");



        getResultSorted(result).forEach(r -> {
            if(n.getAndDecrement() >0) {
                try {
                    csvWriter.write(r.getKey() + ";" + r.getValue() + "\n");
                } catch (IOException err) {
                    err.printStackTrace();
                }
            }else {
                try {
                    csvWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                HazelcastClient.shutdownAll();
                System.exit(0);
            }
        });
        csvWriter.close();

        HazelcastClient.shutdownAll();


    }

    public static Stream<Map.Entry<String, Integer>> getResultSorted(Map<String, Integer> result) {

        return result.entrySet().stream().sorted(
                Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()));

    }

    public static Map<String, Integer> getMapReduceResult(HazelcastInstance hazelcastInstance, IList<Tree> trees) throws ExecutionException, InterruptedException {

        final KeyValueSource<String,Tree> ds = KeyValueSource.fromList(trees);


        JobTracker jt = hazelcastInstance.getJobTracker("g2_jobs");
        Job<String,Tree> job = jt.newJob(ds);


        ICompletableFuture<Map<String,Integer>>  futureResult = job
                .mapper(new NeighborhoodSpeciesMapper())
                .combiner(new SetCombinerFactory<>())
                .reducer(new SetReducerFactory<>())


                .submit(new SetSizeCollator());


        return futureResult.get();
    }


}
