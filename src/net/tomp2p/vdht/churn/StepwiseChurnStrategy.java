package net.tomp2p.vdht.churn;

import net.tomp2p.vdht.Configuration;

/**
 * A churn strategy where the amount of joining and leaving peers is constant
 * (stepwise). Strategy never trespass or undercuts given boundaries.
 * 
 * @author Seppi
 */
public final class StepwiseChurnStrategy implements ChurnStrategy {

	public static final String CHURN_STRATEGY_NAME = "stepwise";

	private final Configuration configuration;

	public StepwiseChurnStrategy(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public int getNumJoiningPeers(int currentNumberOfPeers) {
		if (currentNumberOfPeers + 1 + configuration.getChurnRateJoin() <= configuration.getNumPeersMax()) {
			return configuration.getChurnRateJoin();
		} else {
			return configuration.getNumPeersMax() - (currentNumberOfPeers + 1);
		}
	}

	@Override
	public int getNumLeavingPeers(int currentNumberOfPeers) {
		if (currentNumberOfPeers + 1 - configuration.getChurnRateLeave() >= configuration.getNumPeersMin()) {
			return configuration.getChurnRateLeave();
		} else {
			return currentNumberOfPeers + 1 - configuration.getNumPeersMin();
		}
	}

}
