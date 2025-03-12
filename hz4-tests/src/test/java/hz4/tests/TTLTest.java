package hz4.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.hazelcast.map.ExtendedMapEntry;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

public class TTLTest {

    private static final String TEST_MAP_NAME = "test";

    private static final String KEY1 = "key1";
    private static final String VALUE1 = "value1";
    private static final String VALUE2 = "value2";

    private static HazelcastInstance hazelcastInstance1;
    private static HazelcastInstance hazelcastInstance2;
    private static HazelcastInstance hazelcastInstance3;

    @BeforeClass
    public static void init() {
        Config c = new Config();
        c.setClusterName(TTLTest.class.getName());
        c.setNetworkConfig(new NetworkConfig().setJoin(new JoinConfig().setMulticastConfig(new MulticastConfig().setEnabled(false)).setTcpIpConfig(new TcpIpConfig().setEnabled(true))));
        
        hazelcastInstance1 = Hazelcast.newHazelcastInstance(c);
        InetSocketAddress hz1address = (InetSocketAddress)hazelcastInstance1.getLocalEndpoint().getSocketAddress();
        String hz1addressString = hz1address.getHostString() + ":" + hz1address.getPort();
        
        c.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(Arrays.asList(hz1addressString));
        hazelcastInstance2 = Hazelcast.newHazelcastInstance(c);
        
        InetSocketAddress hz2address = (InetSocketAddress)hazelcastInstance1.getLocalEndpoint().getSocketAddress();
        String hz2addressString = hz2address.getHostString() + ":" + hz2address.getPort();
        c.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(Arrays.asList(hz1addressString, hz2addressString));
        hazelcastInstance3 = Hazelcast.newHazelcastInstance(c);
    }

    @Test
    public void testTTL_Different_With_HZ4() throws InterruptedException {
        assertEquals(3, hazelcastInstance1.getCluster().getMembers().size());
        
        getTestMap(hazelcastInstance1).set(KEY1, VALUE1);
        getTestMap(hazelcastInstance2).setTtl(KEY1, 10, TimeUnit.SECONDS);

        assertEquals(VALUE1, getTestMap(hazelcastInstance3).get(KEY1));
        
        assertEquals(10000L, getTestMap(hazelcastInstance3).getEntryView(KEY1).getTtl());

        hazelcastInstance3.<String,String>getMap(TEST_MAP_NAME).executeOnKey(KEY1, new ChangeValueEntryProcessor(VALUE2));

        assertEquals("ttl was changed by entry processor", 5000, getTestMap(hazelcastInstance1).getEntryView(KEY1).getTtl());

        Thread.sleep(3*1000);
        var value = getTestMap(hazelcastInstance1).getEntryView(KEY1);
        System.out.println("expiration_time=" + value.getExpirationTime());
        assertTrue(value != null);
    }

    public IMap<String,String> getTestMap(HazelcastInstance fromInstance) {
        return fromInstance.<String,String>getMap(TEST_MAP_NAME);
    }


    public static class ChangeValueEntryProcessor implements EntryProcessor<String, String, String> {

        private String newValue;

        public ChangeValueEntryProcessor() {
        }

        public ChangeValueEntryProcessor(String newValue) {
            this.newValue = newValue;
        }

        @Override
        public String process(Entry<String, String> entry) {

            ExtendedMapEntry extendedMapEntry = (ExtendedMapEntry) entry;

            return (String) extendedMapEntry.setValue(newValue, 5, TimeUnit.SECONDS);

        }

    }

    
}