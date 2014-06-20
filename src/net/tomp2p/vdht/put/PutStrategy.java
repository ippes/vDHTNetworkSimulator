package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;

/**
 * Abstraction for different putting approaches.
 * 
 * @author Seppi
 */
public abstract class PutStrategy {

	protected final Number480 key;
	
	public PutStrategy(Number480 key) {
		this.key = key;
	}
	
	public abstract void getUpdateAndPut(PeerDHT peer) throws Exception;

}
