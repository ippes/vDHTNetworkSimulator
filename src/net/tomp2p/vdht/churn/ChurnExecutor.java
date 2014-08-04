package net.tomp2p.vdht.churn;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.LocalNetworkSimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repeatedly executes it-self. Adds and removes peers according given churn
 * strategy and join/leave ratio.
 * 
 * @author Seppi
 */
public final class ChurnExecutor implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(ChurnExecutor.class);

	private final Random random = new Random();
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	private final Configuration configuration;
	private final LocalNetworkSimulator simulator;
	private final ChurnStrategy churnStrategy;

	private boolean shutdown = false;
	private boolean stopped = false;

	public ChurnExecutor(LocalNetworkSimulator simulator) {
		this.configuration = simulator.getConfiguration();
		this.simulator = simulator;
		String churnStrategyName = configuration.getChurnStrategyName();
		switch (churnStrategyName) {
			case StepwiseChurnStrategy.CHURN_STRATEGY_NAME:
				churnStrategy = new StepwiseChurnStrategy(configuration);
				break;
			case StepwiseRandomChurnStrategy.CHURN_STRATEGY_NAME:
				churnStrategy = new StepwiseRandomChurnStrategy(configuration);
				break;
			case WildChurnStrategy.CHURN_STRATEGY_NAME:
				churnStrategy = new WildChurnStrategy(configuration);
				break;
			default:
				churnStrategy = new StepwiseChurnStrategy(configuration);
				logger.warn(
						"An unknown chrun strategy name '{}' was given. Selected '{}' as default churn strategy.",
						churnStrategyName, StepwiseChurnStrategy.CHURN_STRATEGY_NAME);
		}
	}

	public void start() {
		scheduler.schedule(this, delay(), TimeUnit.MILLISECONDS);
	}

	public void shutdown() {
		shutdown = true;
		scheduler.shutdown();
	}

	@Override
	public void run() {
		Thread.currentThread().setName("vDHT - Churn");
		try {
			// toggle join/leaves
			double churnRate = random.nextDouble();
			if (configuration.getChurnJoinLeaveRate() < churnRate) {
				simulator.addPeersToTheNetwork(churnStrategy);
			} else {
				simulator.removePeersFromNetwork(churnStrategy);
			}
		} catch (Exception e) {
			if (!shutdown) {
				logger.error("Caught an unexpected exception.", e);
			}
		} finally {
			if (shutdown) {
				stopped = true;
				return;
			}
			// schedule next churn task with a varying delay
			int delay = delay();
			logger.trace("scheduling in '{}' milliseconds.", delay);
			scheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Calculates a varying delay within given boundaries.
	 * 
	 * @return a delay
	 */
	public int delay() {
		int maxDelta = configuration.getChurnRateMaxDelayInMilliseconds()
				- configuration.getChurnRateMinDelayInMilliseconds();
		int varyingDelta = maxDelta > 0 ? random.nextInt(maxDelta + 1) : 0;
		return configuration.getChurnRateMinDelayInMilliseconds() + varyingDelta;
	}

	public boolean isShutdown() {
		return stopped;
	}
}
