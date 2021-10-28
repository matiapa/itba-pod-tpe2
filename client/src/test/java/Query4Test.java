import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import pod.client.Query3;
import pod.client.Query4;
import pod.models.NeighborPairs;
import pod.models.Tree;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class Query4Test {

    private HazelcastInstance member, client;
    private TestHazelcastFactory hazelcastFactory;


    private final List<Tree> treeList = new ArrayList<>();
//            Arrays.asList(
//            new Tree("PEPE","LINIERS","calle123"),
//            new Tree("PEPE1","LINIERS","calle123"),
//            new Tree("PEPE2","LINIERS","calle123"),
//            new Tree("PEPE3","VILLA LURO","calle123"),
//            new Tree("PEPE4","VILLA LURO","calle123"),
//            new Tree("PEPE5","CABALLITO","calle123"),
//            new Tree("PEPE6","PALERMO","calle123"),
//            new Tree("PEPE7","COLEGIALES","calle123"),
//            new Tree("PEPE8","COLEGIALES","calle123")

    @Before
    public void setUp (){

        hazelcastFactory = new TestHazelcastFactory();
        // Group Config
        GroupConfig groupConfig = new
                GroupConfig().setName("g2").setPassword("g2-pass");
        // Config
        Config config = new Config().setGroupConfig(groupConfig);
        member = hazelcastFactory.newHazelcastInstance(config);
        // Client Config
        ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }



    @Test
    public void testMapReduceResult() throws ExecutionException, InterruptedException {

        for (int i = 0; i < 102; i++) {
            treeList.add(new Tree(String.valueOf(i),"A","street"));
            treeList.add(new Tree(String.valueOf(i),"B","street"));
            treeList.add(new Tree(String.valueOf(i),"C","street"));
        }

        String listName = "trees";
        IList<Tree> testListFromMember = member.getList(listName);
        testListFromMember.addAll(treeList);
        IList<Tree> testList = client.getList(listName);
        List<NeighborPairs> mapRedResult = Query4.getMapReduceResult(client, testList);



        Assert.assertEquals(Optional.of( (long)100).get(), mapRedResult.get(0).getGroup());
        Assert.assertEquals("A", mapRedResult.get(0).getNeighborhoodA());
        Assert.assertEquals("B", mapRedResult.get(0).getNeighborhoodB());
        Assert.assertEquals(Optional.of( (long)100).get(), mapRedResult.get(1).getGroup());
        Assert.assertEquals("A", mapRedResult.get(1).getNeighborhoodA());
        Assert.assertEquals("C", mapRedResult.get(1).getNeighborhoodB());
        Assert.assertEquals(Optional.of( (long)100).get(), mapRedResult.get(2).getGroup());
        Assert.assertEquals("B", mapRedResult.get(2).getNeighborhoodA());
        Assert.assertEquals("C", mapRedResult.get(2).getNeighborhoodB());

    }


    @After
    public void closeHazelcast() {
        hazelcastFactory.shutdownAll();
    }
}

