package net.tomp2p.vdht.churn;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.vdht.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A churn strategy where the amount of joining and leaving peers varies in a
 * certain range. (stepwise). Strategy never trespass or undercuts given
 * boundaries.
 * 
 * @author Seppi
 */
public final class StepwiseRandomChurnStrategy implements ChurnStrategy {

	private final Logger logger = LoggerFactory.getLogger(StepwiseRandomChurnStrategy.class);

	public static final String CHURN_STRATEGY_NAME = "stepwiseRandom";

	private final Random random = new Random();

	private final int numPeersMax;
	private final int numPeersMin;
	private final int churnRateJoin;
	private final int churnRateLeave;

	public StepwiseRandomChurnStrategy() throws IOException {
		numPeersMax = Configuration.getNumPeersMax();
		logger.trace("max # peers = '{}", numPeersMax);
		numPeersMin = Configuration.getNumPeersMin();
		logger.trace("min # peers = '{}", numPeersMin);
		churnRateJoin = Configuration.getChurnRateJoin();
		logger.trace("churn rate join = '{}'", churnRateJoin);
		churnRateLeave = Configuration.getChurnRateLeave();
		logger.trace("churn rate leave = '{}'", churnRateLeave);
	}

	@Override
	public int getNumJoiningPeers(int currentNumberOfPeers) {
		if (currentNumberOfPeers + 1 + churnRateJoin <= numPeersMax) {
			return churnRateJoin > 0 ? random.nextInt(churnRateJoin + 1) : 0;
		} else {
			int restDelta = numPeersMax - (currentNumberOfPeers + 1);
			return restDelta > 0 ? random.nextInt(restDelta + 1) : 0;
		}
	}

	@Override
	public int getNumLeavingPeers(int currentNumberOfPeers) {
		if (currentNumberOfPeers + 1 - churnRateLeave >= numPeersMin) {
			return churnRateLeave > 0 ? random.nextInt(churnRateLeave + 1) : 0;
		} else {
			int restDelta = currentNumberOfPeers + 1 - numPeersMin;
			return restDelta > 0 ? random.nextInt(restDelta + 1) : 0;
		}
	}

}
