package net.tomp2p.vdht.churn;

/**
 * Interface for different churn strategies.
 * 
 * @author Seppi
 */
public interface ChurnStrategy {

	/**
	 * Get the amount of peers which should join the network.
	 * 
	 * @param currentNumberOfPeers
	 *            number of peers which currently are online
	 * @return number of peers which should join the network
	 */
	public int getNumJoiningPeers(int currentNumberOfPeers);

	/**
	 * Get the amount of peers which should leave the network.
	 * 
	 * @param currentNumberOfPeers
	 *            number of peers which currently are online
	 * @return number of peers which should leave the network
	 */
	public int getNumLeavingPeers(int currentNumberOfPeers);

}
