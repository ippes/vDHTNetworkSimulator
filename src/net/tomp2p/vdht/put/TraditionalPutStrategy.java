package net.tomp2p.vdht.put;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.vdht.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A putting strategy which simply overwrites versions with the newer ones. The strategy doesn't use any
 * version keys.
 * 
 * @author Seppi
 */
public final class TraditionalPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(TraditionalPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "traditional";

	private final Configuration configuration;

	public TraditionalPutStrategy(String id, Number480 key, Configuration configuration) {
		super(id, key);
		this.configuration = configuration;
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// fetch from the network
		FutureGet futureGet = peer.get(key.locationKey()).domainKey(key.domainKey())
				.contentKey(key.contentKey()).getLatest().start();
		futureGet.awaitUninterruptibly();

		// get result and update it (append id)
		Map<String, Integer> value;
		Data result = futureGet.data();
		if (result == null) {
			// reset value
			value = new HashMap<String, Integer>();
			value.put(id, 1);
		} else {
			value = (Map<String, Integer>) result.object();
			if (value.containsKey(id)) {
				value.put(id, value.get(id) + 1);
			} else {
				value.put(id, 1);
			}
		}
		Data data = new Data(value);
		if (configuration.getPutTTLInSeconds() > 0) {
			data.ttlSeconds(configuration.getPutTTLInSeconds());
		}

		// put updated version into network
		FuturePut futurePut = peer.put(key.locationKey()).data(key.contentKey(), data)
				.domainKey(key.domainKey()).start();
		futurePut.awaitUninterruptibly();

		putCounter++;

		logger.debug("Executed put. value = '{}' key ='{}' id = '{}'", value, key, id);
	}

	@Override
	public void printResults() {
		// no other statistics to print
	}

}
