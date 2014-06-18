package net.tomp2p.vdht.churn;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.vdht.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A churn strategy where the amount of joining and leaving peers varies.
 * Strategy never trespass or undercuts given boundaries.
 * 
 * @author Seppi
 */
public final class WildChurnStrategy implements ChurnStrategy {

	private final Logger logger = LoggerFactory.getLogger(WildChurnStrategy.class);

	public static final String CHURN_STRATEGY_NAME = "wild";

	private final Random random = new Random();

	private final int numPeersMax;
	private final int numPeersMin;

	public WildChurnStrategy() throws IOException {
		numPeersMax = Configuration.getNumPeersMax();
		logger.trace("max # peers = '{}", numPeersMax);
		numPeersMin = Configuration.getNumPeersMin();
		logger.trace("min # peers = '{}", numPeersMin);
	}

	@Override
	public int getNumJoiningPeers(int currentNumberOfPeers) {
		int maxJoiningPeers = numPeersMax - (currentNumberOfPeers + 1);
		return maxJoiningPeers > 0 ? random.nextInt(maxJoiningPeers) : 0;
	}

	@Override
	public int getNumLeavingPeers(int currentNumberOfPeers) {
		int maxLeavingPeers = currentNumberOfPeers - numPeersMin;
		return maxLeavingPeers > 0 ? random.nextInt(maxLeavingPeers) : 0;
	}

}
