package net.tomp2p.vdht.churn;

import java.util.Random;

import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.LocalNetworkSimulator;

/**
 * A churn strategy where the amount of joining and leaving peers varies in a
 * certain range. (stepwise). Strategy never trespass or undercuts given
 * boundaries.
 * 
 * @author Seppi
 */
public final class StepwiseRandomChurnStrategy implements ChurnStrategy {

	public static final String CHURN_STRATEGY_NAME = "stepwiseRandom";

	private final Random random = new Random();

	private final LocalNetworkSimulator simulator;
	private final Configuration configuration;

	public StepwiseRandomChurnStrategy(LocalNetworkSimulator simulator) {
		this.simulator = simulator;
		this.configuration = simulator.getConfiguration();
	}

	private int getNumJoiningPeers() {
		int currentNumberOfPeers = simulator.getPeerSize();
		if (currentNumberOfPeers + 1 + configuration.getChurnRateJoin() <= configuration
				.getNumPeersMax()) {
			return configuration.getChurnRateJoin() > 0 ? random
					.nextInt(configuration.getChurnRateJoin() + 1) : 0;
		} else {
			int restDelta = configuration.getNumPeersMax()
					- (currentNumberOfPeers + 1);
			return restDelta > 0 ? random.nextInt(restDelta + 1) : 0;
		}
	}

	private int getNumLeavingPeers() {
		int currentNumberOfPeers = simulator.getPeerSize();
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
