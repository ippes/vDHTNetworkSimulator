package net.tomp2p.vdht.put;

import java.util.HashMap;
import java.util.Map;
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

	public TraditionalVersionPutStrategy(String id, Number480 key, Result result, Configuration configuration) {
		super(id, key, result);
		this.configuration = configuration;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// fetch from the network
		FutureGet futureGet = peer.get(key.locationKey()).domainKey(key.domainKey())
				.contentKey(key.contentKey()).getLatest().start();
		futureGet.awaitUninterruptibly();

		// sort result
		NavigableMap<Number640, Data> map = new TreeMap<Number640, Data>(futureGet.dataMap());

		// update value (append task's id)
		Map<String, Integer> value;
		if (map.isEmpty()) {
			// reset value
			value = new HashMap<String, Integer>();
			value.put(id, 1);
		} else {
			// retrieve latest entry
			Entry<Number640, Data> lastEntry = map.lastEntry();
			basedOnKey = lastEntry.getKey().versionKey();
			value = (Map<String, Integer>) lastEntry.getValue().object();
			if (value.containsKey(id)) {
				value.put(id, value.get(id) + 1);
			} else {
				value.put(id, 1);
			}
		}

		// create new data object
		Data data = new Data(value);
		// set based on key
		data.basedOnSet().add(basedOnKey);
		// set ttl
		if (configuration.getPutTTLInSeconds() > 0) {
			data.ttlSeconds(configuration.getPutTTLInSeconds());
		}
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey(basedOnKey, value.toString());

		// put updated version into network
		FuturePut futurePut = peer.put(key.locationKey()).data(key.contentKey(), data)
				.domainKey(key.domainKey()).versionKey(versionKey).start();
		futurePut.awaitUninterruptibly();

		increaseWriteCounter();

		logger.debug("Put. value = '{}' key ='{}' id = '{}'", value, key, id);
	}

}
