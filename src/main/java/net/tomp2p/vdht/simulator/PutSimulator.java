package net.tomp2p.vdht.simulator;

import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.Outcome;
import net.tomp2p.vdht.put.PutCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A put simulator extending the network simulator. Starts and stops a putting
 * coordinator.
 * 
 * @author Seppi
 */
public class PutSimulator extends NetworkSimulator {

	private static Logger logger = LoggerFactory.getLogger(PutSimulator.class);

	private PutCoordinator putCoordinator;

	public PutSimulator(Configuration configuration) {
		super(configuration);
	}

	public Number480 getKey() {
		if (putCoordinator == null) {
			return null;
		}
		return putCoordinator.getKey();
	}

	public void startPutting() {
		putCoordinator = new PutCoordinator(this);
		putCoordinator.start();
		logger.debug("Putting started.");
	}

	public boolean isPuttingRunning() {
		return !putCoordinator.isShutDown();
	}

	public void shutDownPutCoordinators() throws InterruptedException {
		if (putCoordinator != null) {
			putCoordinator.shutdown();
			while (isPuttingRunning()) {
				Thread.sleep(100);
			}
		}
	}

	public void loadAndStoreResults() {
		if (putCoordinator != null) {
			// load latest version
			putCoordinator.loadResults();
			// store settings and results in a file
			Outcome.writeResult(getConfiguration(), putCoordinator.getResult());
		}
	}

	public void printResults() {
		if (putCoordinator != null) {
			putCoordinator.getResult().printResults();
		}
	}

}
