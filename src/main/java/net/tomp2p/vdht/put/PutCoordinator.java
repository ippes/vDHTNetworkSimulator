package net.tomp2p.vdht.put;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.LocalNetworkSimulator;

public final class PutCoordinator {

	private final Number480 key = new Number480(new Random());

	private final LocalNetworkSimulator simulator;
	private final ScheduledExecutorService scheduler;
	private final PutExecutor[] putExecutors;
	private final Result result;

	public PutCoordinator(LocalNetworkSimulator simulator) {
		this.simulator = simulator;
		this.scheduler = Executors.newScheduledThreadPool(simulator
				.getConfiguration().getPutConcurrencyFactor());
		this.putExecutors = new PutExecutor[simulator.getConfiguration()
				.getPutConcurrencyFactor()];
		this.result = new Result(key);
	}

	public void start() {
		for (int i = 0; i < putExecutors.length; i++) {
			String id = String.valueOf((char) ('a' + i));
			PutExecutor putExecutor = new PutExecutor(id, key, result,
					scheduler, simulator);
			putExecutor.start();
			putExecutors[i] = putExecutor;
		}
	}

	public void shutdown() {
		for (int i = 0; i < putExecutors.length; i++) {
			putExecutors[i].shutdown();
		}
		scheduler.shutdown();
	}

	public boolean isShutDown() {
		boolean shutdown = true;
		for (PutExecutor putExecutor : putExecutors) {
			shutdown = shutdown && putExecutor.isShutdown();
		}
		return shutdown;
	}

	public void loadResults() {
		// load latest version
		Map<String, Integer> latestVersion = simulator.get(key);
		result.setLatestVersion(latestVersion);
	}

	public Result getResult() {
		return result;
	}

}
