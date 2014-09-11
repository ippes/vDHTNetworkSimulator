package net.tomp2p.vdht.churn;

import java.util.Random;

import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.simulator.NetworkSimulator;
import net.tomp2p.vdht.simulator.PutSimulator;

public class SpecificChurnStrategy implements ChurnStrategy {

	public static final String CHURN_STRATEGY_NAME = "specific";

	private final Random random = new Random();

	private final NetworkSimulator simulator;
	private final Configuration configuration;

	public SpecificChurnStrategy(NetworkSimulator simulator) {
		this.simulator = simulator;
		this.configuration = simulator.getConfiguration();
	}

	// private int nonLinearRandom(int bound) {
	// List<Integer> list = new ArrayList<Integer>();
	// for (int i = 0; i < bound; i++) {
	// for (int j = 0; j < bound - i; j++) {
	// list.add(i);
	// }
	// }
	// return list.get(random.nextInt(list.size()));
	// }

	private int getNumJoiningPeers() {
		int currentNumberOfPeers = simulator.getNetworkSize();
		int joiningPeers = configuration.getChurnRateJoin();
		if (currentNumberOfPeers + joiningPeers <= configuration
				.getNumPeersMax()) {
			return joiningPeers;
		} else {
			return configuration.getNumPeersMax() - currentNumberOfPeers;
		}
	}

	private int getNumLeavingPeers() {
		int currentNumberOfPeers = simulator.getNetworkSize();
		int leavingPeers = configuration.getChurnRateLeave();
		if (currentNumberOfPeers - leavingPeers >= configuration
				.getNumPeersMin()) {
			return leavingPeers;
		} else {
			return currentNumberOfPeers - configuration.getNumPeersMin();
		}
	}

	@Override
	public void doChurn() {
		Number480 key = ((PutSimulator) simulator).getKey();

		if (key == null)
			return;

		// toggle join/leaves
		double churnRate = random.nextDouble();
		if (configuration.getChurnJoinLeaveRate() < churnRate) {
			simulator.addPeersToTheNetwork(getNumJoiningPeers());
		} else {
			simulator.removeClosePeersFromNetwork(key.locationKey(),
					getNumLeavingPeers());
		}
	}
}
