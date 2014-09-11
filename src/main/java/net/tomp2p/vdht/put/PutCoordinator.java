package net.tomp2p.vdht.put;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.simulator.PutSimulator;

public final class PutCoordinator {

	private final Number480 key = new Number480(new Random());

	private final PutSimulator simulator;
	private final ScheduledExecutorService scheduler;
	private final PutExecutor[] putExecutors;
	private final Result result;

	public PutCoordinator(PutSimulator simulator) {
		this.simulator = simulator;
		this.scheduler = Executors.newScheduledThreadPool(simulator
				.getConfiguration().getPutConcurrencyFactor());
		this.putExecutors = new PutExecutor[simulator.getConfiguration()
				.getPutConcurrencyFactor()];
		this.result = new Result(key);
	}

	public Number480 getKey() {
		return key;
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
		Map<String, Integer> tmp = new HashMap<String, Integer>();
		for (PutExecutor putExecutor : putExecutors) {
			Map<String, Integer> latest = putExecutor.getPutStrategy()
					.getLatest();
			for (String id : latest.keySet()) {
				if (tmp.containsKey(id)) {
					if (tmp.get(id) < latest.get(id)) {
						tmp.put(id, latest.get(id));
					}
				} else {
					tmp.put(id, latest.get(id));
				}
			}
		}
		result.setLatestVersion(tmp);
	}

	public Result getResult() {
		return result;
	}

}
