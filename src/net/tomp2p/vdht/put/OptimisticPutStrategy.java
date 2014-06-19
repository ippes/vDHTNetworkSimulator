package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptimisticPutStrategy implements PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(OptimisticPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "optimistic";

	@Override
	public void getUpdateAndPut(PeerDHT peer) {

	}

}
