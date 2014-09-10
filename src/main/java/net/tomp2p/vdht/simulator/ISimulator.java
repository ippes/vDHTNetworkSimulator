package net.tomp2p.vdht.simulator;

import java.io.IOException;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number160;

public interface ISimulator {

	public int getPeerSize();

	public void createNetwork() throws IOException;

	public void createNetwork(String bootstrapIP, int bootstrapPort)
			throws IOException;

	public void startChurn();

	public boolean isChurnRunning();

	public void shutDownChurn() throws InterruptedException;

	public void shutDownNetwork();

	public PeerDHT requestPeer();

	public void releasePeer(PeerDHT peer);

	public void addPeersToTheNetwork(int numberOfPeerToJoin);

	public void removePeersFromNetwork(int numberOfLeavingPeers);

	public void removeClosePeersFromNetwork(Number160 locationKey,
			int numLeavingPeers);

	public PeerDHT getMasterPeer();

}
