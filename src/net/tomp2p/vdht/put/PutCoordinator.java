package net.tomp2p.vdht.put;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.LocalNetworkSimulator;

public final class PutCoordinator {

	private final Number480 key = new Number480(new Random());

	private final int putId;
	private final Configuration configuration;
	private final LocalNetworkSimulator simulator;
	private final ScheduledExecutorService scheduler;
	private final PutExecutor[] putExecutors;

	public PutCoordinator(int putId, Configuration configuration, LocalNetworkSimulator simulator) {
		this.putId = putId;
		this.configuration = configuration;
		this.simulator = simulator;
		this.scheduler = Executors.newScheduledThreadPool(configuration.getPutConcurrencyFactor());
		this.putExecutors = new PutExecutor[configuration.getPutConcurrencyFactor()];
	}

	public void start() {
		for (int i = 0; i < putExecutors.length; i++) {
			String id = String.valueOf((char) ('a' + i));
			PutExecutor putExecutor = new PutExecutor(putId, id, key, configuration, scheduler, simulator);
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

	public Result loadResults() {
		Result result = new Result(key);

		// load latest version
		Map<String, Integer> latestVersion = simulator.get(key);
		result.setLatestVersion(latestVersion);

		// get number of writes
		for (int i = 0; i < putExecutors.length; i++) {
			int writes = putExecutors[i].getPutStrategy().getPutCounter();
			result.putWriteCounter(putExecutors[i].getId(), writes);
		}

		return result;
	}

}
