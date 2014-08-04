package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;

/**
 * Abstraction for different putting approaches.
 * 
 * @author Seppi
 */
public abstract class PutStrategy {

	protected final String id;
	protected final Number480 key;

	protected int putCounter = 0;

	public PutStrategy(String id, Number480 key) {
		this.id = id;
		this.key = key;
	}

	public abstract void getUpdateAndPut(PeerDHT peer) throws Exception;

	public abstract void printResults();

	public int getPutCounter() {
		return putCounter;
	}

}
