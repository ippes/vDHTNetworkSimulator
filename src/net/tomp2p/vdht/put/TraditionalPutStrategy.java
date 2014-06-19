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
public class TraditionalPutStrategy implements PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(TraditionalPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "traditional";

	private final Number480 key;

	private boolean firstTime = true;
	private long counter = 0;
	private Number160 basedOnKey = Number160.ZERO;
	private String value = "";

	public TraditionalPutStrategy(Number480 key) {
		this.key = key;
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// check if is executing the first time, skip getting
		if (firstTime) {
			firstTime = false;
		} else {
			// fetch from the network
			FutureGet futureGet = peer.get(key.locationKey()).domainKey(key.domainKey())
					.contentKey(key.contentKey()).getLatest().start();
			futureGet.awaitUninterruptibly();

			// sort result
			NavigableMap<Number640, Data> map = new TreeMap<Number640, Data>(futureGet.dataMap());
			if (map.isEmpty()) {
				logger.warn("No result after get. key = '{}'", key);
				return;
			}
			// retrieve latest entry
			Entry<Number640, Data> lastEntry = map.lastEntry();
			basedOnKey = lastEntry.getKey().versionKey();
			value = (String) lastEntry.getValue().object();
		}

		// append next one from alphabet
		char lower = (char) ('a' + (counter % 26));
		value += lower;

		// create new data object
		Data data = new Data(value);
		// set based on key
		data.basedOnSet().add(basedOnKey);
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey();

		// put updated version into network
		FuturePut futurePut = peer.put(key.locationKey()).data(key.contentKey(), data)
				.domainKey(key.domainKey()).versionKey(versionKey).start();
		futurePut.awaitUninterruptibly();
	}

}
