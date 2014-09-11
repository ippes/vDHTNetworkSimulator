package net.tomp2p.vdht.churn;

import java.util.Random;

import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.simulator.NetworkSimulator;

/**
 * A churn strategy where the amount of joining and leaving peers varies.
 * Strategy never trespass or undercuts given boundaries.
 * 
 * @author Seppi
 */
public final class WildChurnStrategy implements ChurnStrategy {

	public static final String CHURN_STRATEGY_NAME = "wild";

	private final Random random = new Random();

	private final NetworkSimulator simulator;
	private final Configuration configuration;

	public WildChurnStrategy(NetworkSimulator simulator) {
		this.simulator = simulator;
		this.configuration = simulator.getConfiguration();
	}

	private int getNumJoiningPeers() {
		int currentNumberOfPeers = simulator.getNetworkSize();
		int maxJoiningPeers = configuration.getNumPeersMax()
				- currentNumberOfPeers;
		return maxJoiningPeers > 0 ? random.nextInt(maxJoiningPeers + 1) : 0;
	}

	private int getNumLeavingPeers() {
		int currentNumberOfPeers = simulator.getNetworkSize();
		int maxLeavingPeers = currentNumberOfPeers
				- configuration.getNumPeersMin();
		return maxLeavingPeers > 0 ? random.nextInt(maxLeavingPeers + 1) + 1
				: 0;
	}

	@Override
	public void doChurn() {
		// toggle join/leaves
		double churnRate = random.nextDouble();
		if (configuration.getChurnJoinLeaveRate() < churnRate) {
			simulator.addPeersToTheNetwork(getNumJoiningPeers());
		} else {
			simulator.removePeersFromNetwork(getNumLeavingPeers());
		}
	}

}
