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
import net.tomp2p.vdht.LocalNetworkSimulator.PutCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple put strategy. Uses the usual {@link PeerDHT#put(Number160)} method
 * without the a vDHT approach.
 * 
 * @author Seppi
 */
public final class TraditionalPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(TraditionalPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "traditional";

	private boolean firstTime = true;
	private Number160 basedOnKey = Number160.ZERO;
	private String value = "";

	public TraditionalPutStrategy(PutCoordinator putCoordinator, Number480 key) {
		super(putCoordinator, key);
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

		// get latest char
		char[] charArray = value.toCharArray();
		char lastChar = charArray.length > 0 ? charArray[charArray.length - 1] : '`';

		// append next one from alphabet
		value += putCoordinator.requestNextChar(lastChar);

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

		logger.debug("Put. value = '{}'", value);
	}

}
