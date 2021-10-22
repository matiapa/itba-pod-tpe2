package pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.models.Neighbourhood;
import pod.models.Tree;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.Stream;

public abstract class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static void logTimestamp(FileWriter fileWriter, String message) throws IOException {
        String timestamp = (new SimpleDateFormat("dd/MM/yyyy hh:mm:ss:SSSS")).format(new Date());
        fileWriter.write(timestamp + " - " + message + "\n");
    }

    public static String parseParameter(String[] args, String paramToFind){
        return Stream.of(args).filter(arg -> arg.contains(paramToFind))
            .map(arg -> arg.substring(arg.indexOf("=")+ 1))
            .findFirst().orElseThrow(() -> new IllegalArgumentException(
                    "Must provide " + paramToFind + "=<value> param")
            );
    }

    public static HazelcastInstance getClientInstance(String[] args) {
        ClientConfig clientConfig = new ClientConfig();

        GroupConfig groupConfig = new GroupConfig()
            .setName("g2")
            .setPassword("g2-pass");
        clientConfig.setGroupConfig(groupConfig);

        String[] servers = parseParameter(args, "-Daddresses").split(";");
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        clientNetworkConfig.addAddress(servers);
        clientConfig.setNetworkConfig(clientNetworkConfig);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public static void loadTreesFromCsv(String[] args, HazelcastInstance hz, FileWriter timestampWriter) throws IOException {
        logTimestamp(timestampWriter, "Inicio de la lectura del archivo");

        String city = parseParameter(args, "-Dcity");
        Path path = Paths.get(parseParameter(args, "-DinPath"));

        IList<Tree> trees = hz.getList("g2_trees");

        int treesLoaded = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            br.readLine();

            String line;
            if(city.equals("BUE")) {
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("[;]");
                    trees.add(new Tree(values[7], values[2], values[4]));
                    treesLoaded++;
                }
            } else if(city.equals("VAN")) {
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("[;]");
                    trees.add(new Tree(values[6], values[12], values[2]));
                    treesLoaded++;
                }
            }

            logger.info("{} trees added", treesLoaded);
        } catch (IOException e) {
            logger.error("Error Opening CSV File");
        }

        logTimestamp(timestampWriter, "Fin de la lectura del archivo");
    }

    public static void loadNeighbourhoodsFromCsv(String[] args, HazelcastInstance hz, FileWriter timestampWriter) throws IOException {
        logTimestamp(timestampWriter, "Inicio de la lectura del archivo");

        String city = parseParameter(args, "-Dcity");
        Path path = Paths.get(parseParameter(args, "-DinPath"));

        IList<Neighbourhood> neighbourhoods = hz.getList("g2_neighbourhoods");

        int neighbourhoodsLoaded = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            br.readLine();

            String line;
            if(city.equals("BUE")) {
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("[;]");
                    neighbourhoods.add(new Neighbourhood(values[0], Integer.parseInt(values[1])));
                    neighbourhoodsLoaded++;
                }
            } else if(city.equals("VAN")) {
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("[;]");
                    neighbourhoods.add(new Neighbourhood(values[0], Integer.parseInt(values[1])));
                    neighbourhoodsLoaded++;
                }
            }

            logger.info("{} neighbourhoods added", neighbourhoodsLoaded);
        } catch (IOException e) {
            logger.error("Error Opening CSV File");
        }

        logTimestamp(timestampWriter, "Fin de la lectura del archivo");
    }


//    public static Stream<Tree> getTreesFromCsv(String[] args) {
//        String city = parseParameter(args, "-Dcity");
//        Path path = Paths.get(parseParameter(args, "-DinPath"));
//
//        try{
//            BufferedReader br = new BufferedReader(new FileReader(path.toFile()));
//
//            if(city.equals("BUE")) {
//                return br.lines().skip(1).map(line -> {
//                    String[] values = line.split("[;]");
//                    return new Tree(values[7], values[2], values[4]);
//                });
//            } else if(city.equals("VAN")) {
//                return br.lines().skip(1).map(line -> {
//                    String[] values = line.split("[;]");
//                    return new Tree(values[6], values[12], values[2]);
//                });
//            } else {
//                throw new IllegalArgumentException("Invalid city");
//            }
//
//        } catch (IOException e) {
//            logger.error("Error Opening CSV File");
//            throw new RuntimeException("Error opening CSV File");
//        }
//    }


}