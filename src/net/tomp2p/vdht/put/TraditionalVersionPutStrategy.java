package net.tomp2p.vdht.put;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple putting strategy using version keys. Strategy fetches latest version, updates it, generates a new
 * version key according the fetched version and puts it.
 * 
 * @author Seppi
 */
public final class TraditionalVersionPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(TraditionalVersionPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "traditionalVersion";

	private final Configuration configuration;

	private Number160 basedOnKey = Number160.ZERO;

	public TraditionalVersionPutStrategy(String id, Number480 key, Configuration configuration) {
		super(id, key);
		this.configuration = configuration;
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// fetch from the network
		FutureGet futureGet = peer.get(key.locationKey()).domainKey(key.domainKey())
				.contentKey(key.contentKey()).getLatest().start();
		futureGet.awaitUninterruptibly();

		// sort result
		NavigableMap<Number640, Data> map = new TreeMap<Number640, Data>(futureGet.dataMap());

		// update value (append task's id)
		String value;
		if (map.isEmpty()) {
			// reset value
			value = id;
		} else {
			// retrieve latest entry
			Entry<Number640, Data> lastEntry = map.lastEntry();
			basedOnKey = lastEntry.getKey().versionKey();
			value = (String) lastEntry.getValue().object() + id;
		}

		// create new data object
		Data data = new Data(value);
		// set based on key
		data.basedOnSet().add(basedOnKey);
		// set ttl
		data.ttlSeconds(configuration.getPutTTLInSeconds());
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey(basedOnKey, value);

		// put updated version into network
		FuturePut futurePut = peer.put(key.locationKey()).data(key.contentKey(), data)
				.domainKey(key.domainKey()).versionKey(versionKey).start();
		futurePut.awaitUninterruptibly();

		// increase put counter
		putCounter++;

		logger.debug("Put. value = '{}' key ='{}' id = '{}'", value, key, id);
	}

	@Override
	public void printResults() {
		// nothing special to print
	}

}
