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
import pod.models.Pair;
import pod.collators.SetSizeCollatorWithOrder;
import pod.combiners.SetCombinerFactory;
import pod.mappers.NeighborhoodSpeciesMapper;
import pod.models.Tree;
import pod.reducers.SetReducerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;


import static pod.client.Utils.parseParameter;

public class Query3 {

    private final static Logger logger = LoggerFactory.getLogger(Query3.class);


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        String outPath = parseParameter(args,"-DoutPath");
        String nParam = parseParameter(args,"-Dn");
        int n=0;
        try {
             n =Integer.parseInt(nParam);

            if (n < 0) {
                logger.error("<n> param must be an integer greater than zero");
                System.exit(1);
            }


        }catch (NumberFormatException e){
            logger.error("<n> param must be an integer greater than zero");
            System.exit(1);

        }

        File logFile = new File(outPath+"/time3.txt");
        FileWriter logWriter = new FileWriter(logFile);


        HazelcastInstance hazelcastInstance = Utils.getClientInstance(args);

        Utils.loadTreesFromCsv(args,hazelcastInstance,logWriter);



        Utils.logTimestamp(logWriter, "Inicio del trabajo map/reduce");

        IList<Tree> trees = hazelcastInstance.getList("g2_trees");


        Stream<Pair<String,Integer>> result = getMapReduceResult(hazelcastInstance,trees,n);



        Utils.logTimestamp(logWriter, "Fin del trabajo map/reduce");

        logWriter.close();
        // Write results

        File csvFile = new File(outPath+"/query3.csv");
        FileWriter csvWriter = new FileWriter(csvFile);

        csvWriter.write("NEIGHBOURHOOD;COMMON_NAME_COUNT\n");


        result.forEach(r -> {

                try {
                    csvWriter.write(r.getLeft() + ";" + r.getRight() + "\n");
                } catch (IOException err) {
                    logger.error(err.getMessage());
                    HazelcastClient.shutdownAll();
                }
            });

        csvWriter.close();

        HazelcastClient.shutdownAll();


    }



    public static Stream<Pair<String, Integer>> getMapReduceResult(HazelcastInstance hazelcastInstance, IList<Tree> trees, int limit) throws ExecutionException, InterruptedException {

        final KeyValueSource<String,Tree> ds = KeyValueSource.fromList(trees);


        JobTracker jt = hazelcastInstance.getJobTracker("g2_jobs");
        Job<String,Tree> job = jt.newJob(ds);


        ICompletableFuture<Stream<Pair<String,Integer>>>  futureResult = job
                .mapper(new NeighborhoodSpeciesMapper())
                .combiner(new SetCombinerFactory<>())
                .reducer(new SetReducerFactory<>())


                .submit(new SetSizeCollatorWithOrder(limit));


        return futureResult.get();
    }


}
