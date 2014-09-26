package net.tomp2p.vdht.churn;

import java.util.Random;

import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.simulator.NetworkSimulator;

/**
 * A churn strategy where the amount of joining and leaving peers varies in a
 * certain range. Strategy never trespass or undercuts given boundaries.
 * 
 * @author Seppi
 */
public final class StepwiseRandomChurnStrategy implements ChurnStrategy {

	public static final String CHURN_STRATEGY_NAME = "stepwiseRandom";

	private final Random random = new Random();

	private final NetworkSimulator simulator;
	private final Configuration configuration;

	public StepwiseRandomChurnStrategy(NetworkSimulator simulator) {
		this.simulator = simulator;
		this.configuration = simulator.getConfiguration();
	}

	private int getNumJoiningPeers() {
		int currentNumberOfPeers = simulator.getNetworkSize();
		if (currentNumberOfPeers + configuration.getChurnRateJoin() <= configuration
				.getNumPeersMax()) {
			return configuration.getChurnRateJoin() > 0 ? random
					.nextInt(configuration.getChurnRateJoin() + 1) : 0;
		} else {
			int restDelta = configuration.getNumPeersMax()
					- currentNumberOfPeers;
			return restDelta > 0 ? random.nextInt(restDelta + 1) : 0;
		}
	}

	private int getNumLeavingPeers() {
		int currentNumberOfPeers = simulator.getNetworkSize();
		if (currentNumberOfPeers + 1 - configuration.getChurnRateLeave() >= configuration
				.getNumPeersMin()) {
			return configuration.getChurnRateLeave() > 0 ? random
					.nextInt(configuration.getChurnRateLeave() + 1) : 0;
		} else {
			int restDelta = currentNumberOfPeers + 1
					- configuration.getNumPeersMin();
			return restDelta > 0 ? random.nextInt(restDelta + 1) : 0;
		}
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
