package pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pod.models.Neighbourhood;
import pod.models.Tree;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class Utils {


    private static final String ARG_TREES_FILE_NAME = "arbolesBUE.csv";
    private static final String ARG_NEIGHBOURHOODS_FILE_NAME = "barriosBUE.csv";
    private static final String CAN_TREES_FILE_NAME = "arbolesVAN.csv";
    private static final String CAN_NEIGHBOURHOODS_FILE_NAME = "barriosVAN.csv";

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);


    public static void logTimestamp(FileWriter fileWriter, String message) throws IOException {
        String timestamp = (new SimpleDateFormat("dd/MM/yyyy hh:mm:ss:SSSS")).format(new Date());
        fileWriter.write(timestamp + " - " + message + "\n");
        System.out.println(timestamp + " - " + message);
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


    private static Map<String, Neighbourhood> loadNeighbourhoodsFromCsv(String city, String dir) throws IOException {
        Path path;
        if(city.equals("BUE"))
            path = Paths.get(dir + "/" + ARG_NEIGHBOURHOODS_FILE_NAME);
        else if(city.equals("VAN"))
            path = Paths.get(dir + "/" + CAN_NEIGHBOURHOODS_FILE_NAME);
        else
            throw new IllegalArgumentException("<city> param must be 'BUE' or 'VAN'");

        Map<String, Neighbourhood> neighbourhoods = new HashMap<>();

        int neighbourhoodsLoaded = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("[;]");
                neighbourhoods.put(values[0], new Neighbourhood(values[0], Integer.parseInt(values[1])));
                neighbourhoodsLoaded++;
            }

            logger.info("{} neighbourhoods added", neighbourhoodsLoaded);
        } catch (IOException e) {
            logger.error("Error Opening CSV File");
        }

        return neighbourhoods;
    }


    public static void loadTreesFromCsv(String[] args, HazelcastInstance hz, FileWriter timestampWriter, Predicate<Tree> treePredicate) throws IOException {
        logTimestamp(timestampWriter, "Inicio de la lectura del archivo");

        String city = parseParameter(args, "-Dcity");
        String dir = parseParameter(args, "-DinPath");

        Path path;
        if(city.equals("BUE")) {
            path = Paths.get(dir + "/" + ARG_TREES_FILE_NAME);
        } else if(city.equals("VAN")) {
            path = Paths.get(dir + "/" + CAN_TREES_FILE_NAME);
        } else
            throw new IllegalArgumentException("<city> param must be 'BUE' or 'VAN'");

        // Cargamos la lista de barrios para filtrar los árboles que se cargan

        Map<String, Neighbourhood> neighbourhoods = loadNeighbourhoodsFromCsv(city, dir);

        // Recorremos el CSV y cargamos los arboles a una lista local

        List<Tree> trees = new ArrayList<>();

        List<String> lines = Files.readAllLines(path);

        int treesLoaded = 0;
        for(String line : lines) {
            String[] values = line.split("[;]");

            Tree tree;
            if(city.equals("BUE")) {
                tree = new Tree(values[7], values[2], values[4]);
            } else {
                tree = new Tree(values[6], values[12], values[2]);
            }

            if(neighbourhoods.containsKey(tree.getNeighbour())){
                if(treePredicate == null || treePredicate.test(tree)) {
                    trees.add(tree);
                    treesLoaded++;
                }
            }
        }

        logger.info("{} trees added", treesLoaded);

        // Limpiamos la lista distribuida por si alguien ejecutó otra query antes con otro predicado y cargamos
        // los elementos de la lista local todos juntos

        IList<Tree> treesDist = hz.getList("g2_trees");
        treesDist.clear();
        treesDist.addAll(trees);

        logTimestamp(timestampWriter, "Fin de la lectura del archivo");
    }


    public static void loadTreesFromCsv(String[] args, HazelcastInstance hz, FileWriter timestampWriter) throws IOException {
        loadTreesFromCsv(args, hz, timestampWriter, null);
    }

}