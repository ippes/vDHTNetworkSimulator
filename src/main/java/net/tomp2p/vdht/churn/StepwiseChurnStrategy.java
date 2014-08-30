package net.tomp2p.vdht.churn;

import java.util.Random;

import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.LocalNetworkSimulator;

/**
 * A churn strategy where the amount of joining and leaving peers is constant
 * (stepwise). Strategy never trespass or undercuts given boundaries.
 * 
 * @author Seppi
 */
public final class StepwiseChurnStrategy implements ChurnStrategy {

	public static final String CHURN_STRATEGY_NAME = "stepwise";

	private final Random random = new Random();

	private final LocalNetworkSimulator simulator;
	private final Configuration configuration;

	public StepwiseChurnStrategy(LocalNetworkSimulator simulator) {
		this.simulator = simulator;
		this.configuration = simulator.getConfiguration();
	}

	private int getNumJoiningPeers() {
		int currentNumberOfPeers = simulator.getPeerSize();
		if (currentNumberOfPeers + 1 + configuration.getChurnRateJoin() <= configuration
				.getNumPeersMax()) {
			return configuration.getChurnRateJoin();
		} else {
			return configuration.getNumPeersMax() - (currentNumberOfPeers + 1);
		}
	}

	private int getNumLeavingPeers() {
		int currentNumberOfPeers = simulator.getPeerSize();
		if (currentNumberOfPeers + 1 - configuration.getChurnRateLeave() >= configuration
				.getNumPeersMin()) {
			return configuration.getChurnRateLeave();
		} else {
			return currentNumberOfPeers + 1 - configuration.getNumPeersMin();
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
