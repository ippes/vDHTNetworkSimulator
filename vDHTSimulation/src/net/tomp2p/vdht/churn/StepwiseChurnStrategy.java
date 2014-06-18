package net.tomp2p.vdht.churn;

import java.io.IOException;

import net.tomp2p.vdht.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A churn strategy where the amount of joining and leaving peers is constant
 * (stepwise). Strategy never trespass or undercuts given boundaries.
 * 
 * @author Seppi
 */
public final class StepwiseChurnStrategy implements ChurnStrategy {

	private final Logger logger = LoggerFactory.getLogger(StepwiseChurnStrategy.class);

	public static final String CHURN_STRATEGY_NAME = "stepwise";

	private final int numPeersMax;
	private final int numPeersMin;
	private final int churnRateJoin;
	private final int churnRateLeave;

	public StepwiseChurnStrategy() throws IOException {
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
			return churnRateJoin;
		} else {
			return numPeersMax - (currentNumberOfPeers + 1);
		}
	}

	@Override
	public int getNumLeavingPeers(int currentNumberOfPeers) {
		if (currentNumberOfPeers + 1 - churnRateLeave >= numPeersMin) {
			return churnRateLeave;
		} else {
			return currentNumberOfPeers + 1 - numPeersMin;
		}
	}

}
