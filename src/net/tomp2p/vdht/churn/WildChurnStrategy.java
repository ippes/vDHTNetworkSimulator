package net.tomp2p.vdht.churn;

import java.util.Random;

import net.tomp2p.vdht.Configuration;

/**
 * A churn strategy where the amount of joining and leaving peers varies.
 * Strategy never trespass or undercuts given boundaries.
 * 
 * @author Seppi
 */
public final class WildChurnStrategy implements ChurnStrategy {

	public static final String CHURN_STRATEGY_NAME = "wild";

	private final Random random = new Random();

	private final Configuration configuration;

	public WildChurnStrategy(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public int getNumJoiningPeers(int currentNumberOfPeers) {
		int maxJoiningPeers = configuration.getNumPeersMax() - (currentNumberOfPeers + 1);
		return maxJoiningPeers > 0 ? random.nextInt(maxJoiningPeers + 1) : 0;
	}

	@Override
	public int getNumLeavingPeers(int currentNumberOfPeers) {
		int maxLeavingPeers = currentNumberOfPeers - configuration.getNumPeersMin();
		return maxLeavingPeers > 0 ? random.nextInt(maxLeavingPeers + 1) + 1 : 0;
	}

}
