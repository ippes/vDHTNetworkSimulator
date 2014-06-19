package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;

/**
 * Interface for different putting approaches.
 * 
 * @author Seppi
 */
public interface PutStrategy {

	public void getUpdateAndPut(PeerDHT peer) throws Exception;

}
