package net.tomp2p.vdht.churn;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.LocalNetworkSimulator;

public class SpecificChurnStrategy implements ChurnStrategy {

	public static final String CHURN_STRATEGY_NAME = "specific";

	private final Random random = new Random();

	private final LocalNetworkSimulator simulator;
	private final Configuration configuration;

	public SpecificChurnStrategy(LocalNetworkSimulator simulator) {
		this.simulator = simulator;
		this.configuration = simulator.getConfiguration();
	}

	private int nonLinearRandom(int bound) {
		List<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < bound; i++) {
			for (int j = 0; j < bound - i; j++) {
				list.add(i);
			}
		}
		return list.get(random.nextInt(list.size()));
	}

	private int getNumJoiningPeers() {
		int currentNumberOfPeers = simulator.getPeerSize();
		if (currentNumberOfPeers + 1 + configuration.getChurnRateJoin() <= configuration
				.getNumPeersMax()) {
			return configuration.getChurnRateJoin() > 0 ? nonLinearRandom(configuration.getChurnRateJoin() + 1) : 0;
		} else {
			int restDelta = configuration.getNumPeersMax()
					- (currentNumberOfPeers + 1);
			return restDelta > 0 ? nonLinearRandom(restDelta + 1) : 0;
		}
	}

	private int getNumLeavingPeers() {
		int currentNumberOfPeers = simulator.getPeerSize();
		if (currentNumberOfPeers + 1 - configuration.getChurnRateLeave() >= configuration
				.getNumPeersMin()) {
			return configuration.getChurnRateLeave() > 0 ? nonLinearRandom(configuration.getChurnRateLeave() + 1) : 0;
		} else {
			int restDelta = currentNumberOfPeers + 1
					- configuration.getNumPeersMin();
			return restDelta > 0 ? nonLinearRandom(restDelta + 1) : 0;
		}
	}

	@Override
	public void doChurn() {
		if (simulator.getPutCoordinator() != null) {
			Number480 key = simulator.getPutCoordinator().getKey();
			// toggle join/leaves
			double churnRate = random.nextDouble();
			if (configuration.getChurnJoinLeaveRate() < churnRate) {
				// target the replica nodes of given key to create a new peer
				// the idea is to target the same LocalNetworkSimulator instance
				// to avoid an erasion of peers in the peer list
				simulator.sendCreateMessages(key.locationKey(),
						getNumJoiningPeers());
			} else {
				// target the replica nodes of given key to shutdown
				simulator.sendShutdownMessages(key.locationKey(),
						getNumLeavingPeers());
			}
		}
	}
}
