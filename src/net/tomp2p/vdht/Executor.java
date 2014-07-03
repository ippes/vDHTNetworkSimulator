package net.tomp2p.vdht;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract and runnable class for a repeating scheduled task. Schedules {@link Executor#execute()}.
 * Following tasks are appended by class itself.
 * That's why varying delays are possible.
 * 
 * @author Seppi
 */
public abstract class Executor implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(Executor.class);

	private final Random random = new Random();

	private ScheduledFuture<?> scheduledFuture;

	private final ScheduledExecutorService scheduler;
	private final int minDelayInMilliseconds;
	private final int maxDelayInMilliseconds;
	private final int numberExecutes;

	private boolean shutdown = false;

	protected long counterExecutes = 0;

	/**
	 * Constructor.
	 * 
	 * @param minDelayInMilliseconds
	 *            minimum delay between two schedules
	 * @param maxDelayInMilliseconds
	 *            maximum delay between two schedules
	 * @param numberExecutes
	 *            amount of total executions till it stops, <code>-1</code> for
	 *            endless execution
	 * @throws IOException
	 */
	public Executor(ScheduledExecutorService scheduler, int minDelayInMilliseconds,
			int maxDelayInMilliseconds, int numberExecutes) throws IOException {
		this.scheduler = scheduler;
		this.minDelayInMilliseconds = minDelayInMilliseconds;
		logger.trace("min put delay in milliseconds = '{}'", minDelayInMilliseconds);
		this.maxDelayInMilliseconds = maxDelayInMilliseconds;
		logger.trace("max put delay in milliseconds = '{}'", maxDelayInMilliseconds);
		this.numberExecutes = numberExecutes;
	}

	public void start() {
		scheduledFuture = scheduler.schedule(this, delay(), TimeUnit.MILLISECONDS);
	}

	public void shutdown() {
		shutdown = true;

		if (scheduledFuture != null) {
			scheduledFuture.cancel(true);
		}
		scheduler.shutdownNow();
	}

	public abstract void execute() throws Exception;

	@Override
	public void run() {
		try {
			// execute task
			execute();
		} catch (Exception e) {
			if (shutdown) {
				return;
			}

			logger.error("Caught an unexpected exception.", e);
		} finally {
			if (shutdown) {
				return;
			}

			// check if a rescheduling is needed
			if (counterExecutes < 0) {
				shutdown = true;
				return;
			} else if (counterExecutes == numberExecutes) {
				shutdown = true;
				return;
			} else {
				// increase counter
				counterExecutes++;
			}

			// schedule next task with a varying delay
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
		int maxDelta = maxDelayInMilliseconds - minDelayInMilliseconds;
		int varyingDelta = maxDelta > 0 ? random.nextInt(maxDelta + 1) : 0;
		return minDelayInMilliseconds + varyingDelta;
	}

	/**
	 * Get number of executions of this task.
	 * 
	 * @return execution counter
	 */
	public long getExecutionCounter() {
		return counterExecutes;
	}

	public boolean isShutdown() {
		return shutdown;
	}
}
