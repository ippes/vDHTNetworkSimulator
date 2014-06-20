package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptimisticPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(OptimisticPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "optimistic";

	public OptimisticPutStrategy(Number480 key) {
		super(key);
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {

	}

}
