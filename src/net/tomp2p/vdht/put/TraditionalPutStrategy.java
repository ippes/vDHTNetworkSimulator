package net.tomp2p.vdht.put;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TraditionalPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(TraditionalPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "traditional";

	private String value = "";

	public TraditionalPutStrategy(String id, Number480 key) {
		super(id, key);
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// fetch from the network
		FutureGet futureGet = peer.get(key.locationKey()).domainKey(key.domainKey())
				.contentKey(key.contentKey()).getLatest().start();
		futureGet.awaitUninterruptibly();

		// get result
		Data result = futureGet.data();
		if (result == null) {
			// reset value
			value = "";
		} else {
			value = (String) result.object();
		}

		// append task's id
		value += id;

		// create new data object
		Data data = new Data(value);

		// put updated version into network
		FuturePut futurePut = peer.put(key.locationKey()).data(key.contentKey(), data)
				.domainKey(key.domainKey()).start();
		futurePut.awaitUninterruptibly();

		logger.trace("Executed put. value = '{}' key ='{}' id = '{}'", value, key, id);
	}

}
