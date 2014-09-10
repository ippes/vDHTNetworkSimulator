package net.tomp2p.vdht.put;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.simulator.PutSimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PutCoordinator {

	private static Logger logger = LoggerFactory
			.getLogger(PutCoordinator.class);

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

	@SuppressWarnings("unchecked")
	public void loadResults() {
		// load latest version
		PeerDHT peer = simulator.requestPeer();
		try {
			// load latest string
			FutureGet futureGet = peer.get(key.locationKey())
					.contentKey(key.contentKey()).domainKey(key.domainKey())
					.getLatest().start();
			futureGet.awaitUninterruptibly();
			result.setLatestVersion((Map<String, Integer>) futureGet.data()
					.object());
		} catch (Exception e) {
			logger.error("Couldn't get result.", e);
		} finally {
			simulator.releasePeer(peer);
		}
	}

	public Result getResult() {
		return result;
	}

}
