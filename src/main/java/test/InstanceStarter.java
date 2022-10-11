package test;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;

public class InstanceStarter {
    public static final String CACHE_NAME = "WHOA";

    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            
            CacheConfiguration<Integer, String> conf = new CacheConfiguration<>(CACHE_NAME);
            conf.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            conf.setBackups(1);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(conf)) {
            	
            	if(ignite.cluster().nodes().size()==1) {

            		createCQ(cache);
            		createCQ(cache);
            		createCQ(cache);
            		Thread.sleep(Integer.MAX_VALUE);

            	} else {
            		int keyCnt = 1_000_000;

        			for (int i = 0; i < keyCnt; i++)
        				cache.put(i, Integer.toString(i));
        			
        			for (int i = 0; i < keyCnt; i++)
        				cache.remove(i);
            	}
            }
        }
    }

	private static void createCQ(IgniteCache<Integer, String> cache) {
		ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

		// Callback that is called locally when update notifications are received.
		qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
			@Override
			public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) {}
		});
		cache.query(qry);
	}
}
