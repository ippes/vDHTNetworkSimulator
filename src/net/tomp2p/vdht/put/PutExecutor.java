package net.tomp2p.vdht.put;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.LocalNetworkSimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PutExecutor implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(PutExecutor.class);

	private final Random random = new Random();

	private boolean shutdown = false;
	private boolean stopped = false;
	private long counterExecutes = 0;

	private final int putId;
	private final String id;
	private final Configuration configuration;
	private final ScheduledExecutorService scheduler;
	private final PutStrategy putStrategy;
	private final LocalNetworkSimulator simulator;

	public PutExecutor(int putId, String id, Number480 key, Configuration configuration,
			ScheduledExecutorService scheduler, LocalNetworkSimulator simulator) {
		this.putId = putId;
		this.id = id;
		this.configuration = configuration;
		this.scheduler = scheduler;
		this.simulator = simulator;

		String putApproach = configuration.getPutStrategyName();
		switch (putApproach) {
			case TraditionalPutStrategy.PUT_STRATEGY_NAME:
				putStrategy = new TraditionalPutStrategy(id, key, configuration);
				break;
			case TraditionalVersionPutStrategy.PUT_STRATEGY_NAME:
				putStrategy = new TraditionalVersionPutStrategy(id, key, configuration);
				break;
			case OptimisticPutStrategy.PUT_STRATEGY_NAME:
				putStrategy = new OptimisticPutStrategy(id, key, configuration);
				break;
			case PesimisticPutStrategy.PUT_STRATEGY_NAME:
				putStrategy = new PesimisticPutStrategy(id, key, configuration);
				break;
			default:
				putStrategy = new OptimisticPutStrategy(id, key, configuration);
				logger.warn("An unknown put approach '{}' was given. Selected '{}' as default put approach.",
						putApproach, OptimisticPutStrategy.PUT_STRATEGY_NAME);
		}
	}

	public void start() {
		scheduler.schedule(this, delay(), TimeUnit.MILLISECONDS);
	}

	public void shutdown() {
		shutdown = true;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("vDHT - Put " + putId + "-" + id);
		try {
			simulator.put(putStrategy);
		} catch (Exception e) {
			if (!shutdown) {
				logger.error("Caught an unexpected exception.", e);
			}
		} finally {
			if (shutdown) {
				stopped = true;
				return;
			} else if (counterExecutes == configuration.getNumPuts()) {
				shutdown = true;
				stopped = true;
				return;
			}

			// increase counter
			counterExecutes++;

			// schedule next task with a varying delay
			int delay = delay();
			logger.trace("scheduling in '{}' milliseconds.", delay);
			scheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
		}
	}

	public int delay() {
		int maxDelta = configuration.getPutDelayMaxInMilliseconds()
				- configuration.getPutDelayMinInMilliseconds();
		int varyingDelta = maxDelta > 0 ? random.nextInt(maxDelta + 1) : 0;
		return configuration.getPutDelayMinInMilliseconds() + varyingDelta;
	}

	public boolean isShutdown() {
		return stopped;
	}

	public PutStrategy getPutStrategy() {
		return putStrategy;
	}

	public String getId() {
		return id;
	}

}