package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.LocalNetworkSimulator.PutCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PesimisticPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(PesimisticPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "pesimistic";
	
	public PesimisticPutStrategy(PutCoordinator putCoordinator, Number480 key) {
		super(putCoordinator, key);
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception{

	}

}
