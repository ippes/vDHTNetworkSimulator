package net.tomp2p.vdht.put;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.simulator.PutSimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PutExecutor implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(PutExecutor.class);

	private final Random random = new Random();

	private boolean shutdown = false;
	private boolean stopped = false;

	private long startTime;

	private final String id;
	private final Result result;
	private final Configuration configuration;
	private final ScheduledExecutorService scheduler;
	private final PutStrategy putStrategy;
	private final PutSimulator simulator;

	public PutExecutor(String id, Number480 key, Result result,
			ScheduledExecutorService scheduler, PutSimulator simulator) {
		this.id = id;
		this.result = result;
		this.configuration = simulator.getConfiguration();
		this.scheduler = scheduler;
		this.simulator = simulator;

		String putStrategyName = configuration.getPutStrategyName();
		switch (putStrategyName) {
		case TraditionalPutStrategy.PUT_STRATEGY_NAME:
			putStrategy = new TraditionalPutStrategy(id, key, result,
					configuration);
			break;
		case TraditionalVersionPutStrategy.PUT_STRATEGY_NAME:
			putStrategy = new TraditionalVersionPutStrategy(id, key, result,
					configuration);
			break;
		case OptimisticPutStrategy.PUT_STRATEGY_NAME:
			putStrategy = new OptimisticPutStrategy(id, key, result,
					configuration);
			break;
		case PesimisticPutStrategy.PUT_STRATEGY_NAME:
			putStrategy = new PesimisticPutStrategy(id, key, result,
					configuration);
			break;
		default:
			throw new IllegalArgumentException("Unkown put strategy name '"
					+ putStrategyName + "'");
		}
	}

	public void start() {
		startTime = System.currentTimeMillis();
		scheduler.schedule(this, delay(), TimeUnit.MILLISECONDS);
	}

	public void shutdown() {
		shutdown = true;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("vDHT - Put " + id);
		try {
			simulator.put(putStrategy);
		} catch (Exception e) {
			if (!shutdown) {
				logger.error("Caught an unexpected exception.", e);
			}
		} finally {
			if (shutdown
					|| putStrategy.getWriteCounter()
							+ putStrategy.getMergeCounter() == configuration
							.getNumPuts()) {
				// store runtime
				long runtime = System.currentTimeMillis() - startTime;
				result.storeRuntime(id, runtime);

				shutdown = true;
				stopped = true;
				return;
			}

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