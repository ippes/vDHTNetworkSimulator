package net.tomp2p.vdht.churn;

import java.util.Random;

import net.tomp2p.vdht.Configuration;

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

	private final Configuration configuration;

	public StepwiseRandomChurnStrategy(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public int getNumJoiningPeers(int currentNumberOfPeers) {
		if (currentNumberOfPeers + 1 + configuration.getChurnRateJoin() <= configuration.getNumPeersMax()) {
			return configuration.getChurnRateJoin() > 0 ? random
					.nextInt(configuration.getChurnRateJoin() + 1) : 0;
		} else {
			int restDelta = configuration.getNumPeersMax() - (currentNumberOfPeers + 1);
			return restDelta > 0 ? random.nextInt(restDelta + 1) : 0;
		}
	}

	@Override
	public int getNumLeavingPeers(int currentNumberOfPeers) {
		if (currentNumberOfPeers + 1 - configuration.getChurnRateLeave() >= configuration.getNumPeersMin()) {
			return configuration.getChurnRateLeave() > 0 ? random
					.nextInt(configuration.getChurnRateLeave() + 1) : 0;
		} else {
			int restDelta = currentNumberOfPeers + 1 - configuration.getNumPeersMin();
			return restDelta > 0 ? random.nextInt(restDelta + 1) : 0;
		}
	}

}
