package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PesimisticPutStrategy implements PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(PesimisticPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "pesimistic";

	@Override
	public void getUpdateAndPut(PeerDHT peer) {

	}

}
