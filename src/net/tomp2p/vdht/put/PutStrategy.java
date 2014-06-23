package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.LocalNetworkSimulator.PutCoordinator;

/**
 * Abstraction for different putting approaches.
 * 
 * @author Seppi
 */
public abstract class PutStrategy {

	protected final PutCoordinator putCoordinator;
	protected final Number480 key;

	public PutStrategy(PutCoordinator putCoordinator, Number480 key) {
		this.putCoordinator = putCoordinator;
		this.key = key;
	}

	public abstract void getUpdateAndPut(PeerDHT peer) throws Exception;

}
