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
import net.tomp2p.vdht.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple put strategy. Uses the usual {@link PeerDHT#put(Number160)} method
 * without the a vDHT approach.
 * 
 * @author Seppi
 */
public final class TraditionalVersionPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(TraditionalVersionPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "traditionalVersion";

	private String value = "";
	private Number160 basedOnKey = Number160.ZERO;

	public TraditionalVersionPutStrategy(String id, Number480 key) {
		super(id, key);
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// fetch from the network
		FutureGet futureGet = peer.get(key.locationKey()).domainKey(key.domainKey())
				.contentKey(key.contentKey()).getLatest().start();
		futureGet.awaitUninterruptibly();

		// sort result
		NavigableMap<Number640, Data> map = new TreeMap<Number640, Data>(futureGet.dataMap());
		if (map.isEmpty()) {
			// reset value
			value = "";
		} else {
			// retrieve latest entry
			Entry<Number640, Data> lastEntry = map.lastEntry();
			basedOnKey = lastEntry.getKey().versionKey();
			value = (String) lastEntry.getValue().object();
		}

		// append task's id
		value += id;

		// create new data object
		Data data = new Data(value);
		// set based on key
		data.basedOnSet().add(basedOnKey);
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey(basedOnKey, value);

		// put updated version into network
		FuturePut futurePut = peer.put(key.locationKey()).data(key.contentKey(), data)
				.domainKey(key.domainKey()).versionKey(versionKey).start();
		futurePut.awaitUninterruptibly();

		logger.debug("Put. value = '{}' key ='{}' id = '{}'", value, key, id);
	}

}