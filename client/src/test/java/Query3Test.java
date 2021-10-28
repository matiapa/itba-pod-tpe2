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
import pod.models.Tree;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class Query3Test {


    private HazelcastInstance member, client;
    private TestHazelcastFactory hazelcastFactory;


    private final List<Tree> treeList = Arrays.asList(
            new Tree("PEPE","LINIERS","calle123"),
            new Tree("PEPE1","LINIERS","calle123"),
            new Tree("PEPE2","LINIERS","calle123"),
            new Tree("PEPE3","VILLA LURO","calle123"),
            new Tree("PEPE4","VILLA LURO","calle123"),
            new Tree("PEPE5","CABALLITO","calle123"),
            new Tree("PEPE6","PALERMO","calle123"),
            new Tree("PEPE7","COLEGIALES","calle123"),
            new Tree("PEPE8","COLEGIALES","calle123")

            );
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
        String listName = "trees";
        IList<Tree> testListFromMember = member.getList(listName);
        testListFromMember.addAll(treeList);
        IList<Tree> testList = client.getList(listName);
        Map<String,Integer> mapRedResult = Query3.getMapReduceResult(client,testList);


        Assert.assertEquals(3, (int) mapRedResult.get("LINIERS"));
        Assert.assertEquals(2, (int) mapRedResult.get("VILLA LURO"));
        Assert.assertEquals(1, (int) mapRedResult.get("CABALLITO"));
        Assert.assertEquals(1, (int) mapRedResult.get("PALERMO"));
        Assert.assertEquals(2, (int) mapRedResult.get("COLEGIALES"));

    }

    @Test
    public void testOrder(){
        Map<String,Integer> resultToOrder = new HashMap<>();
        resultToOrder.put("Bruno",3);
        resultToOrder.put("Santi",5);
        resultToOrder.put("Mati",4);
        resultToOrder.put("Iña",5);

        List<String> expectedNames = Arrays.asList("Iña","Santi","Mati","Bruno");
        List<Integer> expectedInts = Arrays.asList(5,5,4,3);

        AtomicInteger i = new AtomicInteger();
        Query3.getResultSorted(resultToOrder).forEach(r->{
            Assert.assertEquals(expectedNames.get(i.get()),r.getKey());
            Assert.assertEquals(expectedInts.get(i.get()),r.getValue());
            i.getAndIncrement();
        });
    }







    @After
    public void closeHazelcast() {
        hazelcastFactory.shutdownAll();
    }
}
