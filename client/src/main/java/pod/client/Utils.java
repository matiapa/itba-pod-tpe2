package pod.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.Neighbourhood;
import pod.Tree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

public abstract class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static String parseParameter (String[] args, String paramToFind){
        return Stream.of(args).filter(arg -> arg.contains(paramToFind))
                .map(arg -> arg.substring(arg.indexOf("=")+ 1))
                .findFirst().orElseThrow(() -> new IllegalArgumentException(
                        "Must provide " + paramToFind + "=<value> param")
                );
    }


    private static void loadTreesFromCsv(Path path, HazelcastInstance hz, DataSource source) {

        IList<Tree> trees = hz.getList("g2_trees");

        int treesLoaded = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("[;]");
                if(source == DataSource.BUENOS_AIRES) {
                    trees.add(new Tree(values[7], values[2], values[4]));
                } else if(source == DataSource.VANCOUVER) {
                    trees.add(new Tree(values[6], values[12], values[2]));
                }
                treesLoaded++;
            }

            logger.info("{} trees added", treesLoaded);
        } catch (IOException e) {
            logger.error("Error Opening CSV File");
        }

    }

    private static void loadNeighbourhoodsFromCsv(Path path, HazelcastInstance hz, DataSource source) {

        IList<Neighbourhood> neighbourhoods = hz.getList("g2_neighbourhoods");

        int neighbourhoodsLoaded = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("[;]");
                if(source == DataSource.BUENOS_AIRES) {
                    neighbourhoods.add(new Neighbourhood(values[0], Integer.parseInt(values[1])));
                } else if(source == DataSource.VANCOUVER) {
                    neighbourhoods.add(new Neighbourhood(values[0], Integer.parseInt(values[1])));
                }
                neighbourhoodsLoaded++;
            }

            logger.info("{} neighbourhoods added", neighbourhoodsLoaded);
        } catch (IOException e) {
            logger.error("Error Opening CSV File");
        }

    }

    public enum DataSource {BUENOS_AIRES, VANCOUVER}

}