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

	private int putCounter = 0;

	public TraditionalPutStrategy(String id, Number480 key) {
		super(id, key);
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// fetch from the network
		FutureGet futureGet = peer.get(key.locationKey()).domainKey(key.domainKey())
				.contentKey(key.contentKey()).getLatest().start();
		futureGet.awaitUninterruptibly();

		// get result and update it (append id)
		String value;
		Data result = futureGet.data();		
		if (result == null) {
			// reset value
			value = id;
		} else {
			value = (String) result.object() + id;
		}

		// put updated version into network
		FuturePut futurePut = peer.put(key.locationKey()).data(key.contentKey(), new Data(value))
				.domainKey(key.domainKey()).start();
		futurePut.awaitUninterruptibly();

		putCounter++;

		logger.debug("Executed put. value = '{}' key ='{}' id = '{}'", value, key, id);
	}

	@Override
	public void printResults() {

		logger.debug("# puts = '{}'", putCounter);
	}

}
