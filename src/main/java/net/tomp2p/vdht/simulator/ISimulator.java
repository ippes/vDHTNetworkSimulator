package net.tomp2p.vdht.simulator;

import java.io.IOException;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number160;

/**
 * Interface for a network simulator, which executes churn.
 * 
 * @author Seppi
 */
public interface ISimulator {

	/**
	 * Returns the number of peers currently online in the network. Master peer
	 * is excluded.
	 * 
	 * @return network size
	 */
	public int getNetworkSize();

	public void createNetwork() throws IOException;

	public void createNetwork(String bootstrapIP, int bootstrapPort)
			throws IOException;

	public void startChurn();

	public boolean isChurnRunning();

	public void shutDownChurn() throws InterruptedException;

	public void shutDownNetwork();

	public PeerDHT requestPeer(boolean permanent);

	public void releasePeer(PeerDHT peer);

	public void addPeersToTheNetwork(int numberOfPeerToJoin);

	public void removePeersFromNetwork(int numberOfLeavingPeers);

	public void removeClosePeersFromNetwork(Number160 locationKey,
			int numLeavingPeers);

	public PeerDHT getMasterPeer();

}
