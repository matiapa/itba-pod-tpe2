package pod.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info("Server starting...");
        Config config = new Config();

        // Group Config

        GroupConfig groupConfig = new GroupConfig().setName("g2").setPassword("g2-pass");
        config.setGroupConfig(groupConfig);

        // Network Config

        MulticastConfig multicastConfig = new MulticastConfig();

        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);

        InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(Collections.singletonList("192.168.1.*"))
                .setEnabled(true);

        NetworkConfig networkConfig = new NetworkConfig()
                .setInterfaces(interfacesConfig)
                .setJoin(joinConfig);

        config.setNetworkConfig(networkConfig);

        // Management Center Config

        // TODO: This is throwing an exception, check why
//        ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
//                .setUrl("http://localhost:32768/mancenter/")
//                .setEnabled(true);
//        config.setManagementCenterConfig(managementCenterConfig);

        // Start cluster

        Hazelcast.newHazelcastInstance(config);
        logger.info("Server started");
    }


}
