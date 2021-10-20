package pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import static pod.client.Utils.parseParameter;

public class Query3 {

    public static void main(String[] args) {
        // Client Config
        ClientConfig clientConfig = new ClientConfig();
        // Group Config
        GroupConfig groupConfig = new
                GroupConfig().setName("g2").setPassword("g2-pass");
        clientConfig.setGroupConfig(groupConfig);
        // Client Network Config
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        String[] addresses = {"192.168.1.51:5701"};
        clientNetworkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);
        HazelcastInstance hazelcastInstance =
                HazelcastClient.newHazelcastClient(clientConfig);







        String serverAddress = parseParameter(args,"-Daddresses");
        String serverPort = serverAddress.substring(serverAddress.indexOf(':')+1);
        serverAddress= serverAddress.substring(0,serverAddress.indexOf(':'));
        String inPath = parseParameter(args,"-DinPath");
        String outPath = parseParameter(args,"-outPath");
        int n  = Integer.parseInt(parseParameter(args,"-Dn"));


        HazelcastClient.shutdownAll();


    }

}
