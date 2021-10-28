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
import pod.models.Pair;
import pod.models.Tree;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class Query3Test {


    private HazelcastInstance member, client;
    private TestHazelcastFactory hazelcastFactory;


    private final List<Tree> treeList = Arrays.asList(
            new Tree("PEPE", "LINIERS", "calle123"),
            new Tree("PEPE1", "LINIERS", "calle123"),
            new Tree("PEPE2", "LINIERS", "calle123"),
            new Tree("PEPE3", "VILLA LURO", "calle123"),
            new Tree("PEPE4", "VILLA LURO", "calle123"),
            new Tree("PEPE5", "CABALLITO", "calle123"),
            new Tree("PEPE6", "PALERMO", "calle123"),
            new Tree("PEPE7", "COLEGIALES", "calle123"),
            new Tree("PEPE8", "COLEGIALES", "calle123")

    );

    @Before
    public void setUp() {

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
        String listName = "trees";
        IList<Tree> testListFromMember = member.getList(listName);
        testListFromMember.addAll(treeList);
        IList<Tree> testList = client.getList(listName);


        SortedSet<Pair<String, Integer>> mapRedResult = Query3.getMapReduceResult(client, testList);

        List<Pair<String, Integer>> expectedResult = Arrays.asList(
                new Pair<>("LINIERS", 3),
                new Pair<>("COLEGIALES", 2),
                new Pair<>("VILLA LURO", 2),
                new Pair<>("CABALLITO", 1),
                new Pair<>("PALERMO", 1)
        );

        AtomicInteger i = new AtomicInteger();
        mapRedResult.forEach(r -> {
            Assert.assertEquals(expectedResult.get(i.get()).getLeft(), r.getLeft());
            Assert.assertEquals(expectedResult.get(i.get()).getRight(), r.getRight());
            i.getAndIncrement();
        });


    }

    @After
    public void closeHazelcast() {
        hazelcastFactory.shutdownAll();

    }

}