package net.tomp2p.vdht;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes and runs a network simulator. Starts churn. Starts putting
 * procedure. Waits a certain amount so that simulation can run. Stops
 * simulation and shutdowns network.
 * 
 * @author Seppi
 */
public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		try {
			// initialize simulation
			LocalNetworkSimulator network = new LocalNetworkSimulator();

			logger.info("Setting up the network simulator.");
			network.createNetwork();

			logger.info("Start simulating churn.");
			network.startChurn();

			logger.info("Start putting data.");
			network.startPutting();

			int runtimeInMilliseconds = Configuration.getRuntimeInMilliseconds();
			if (runtimeInMilliseconds > 0) {
				logger.info("Running simulation for {} milliseconds.", runtimeInMilliseconds);
				Thread.sleep(runtimeInMilliseconds);
			} else {
				while (network.isRunning()) {
					Thread.sleep(500);
				}
			}

			logger.info("Stopping network simulator.");
			network.shutDown();

			logger.info("Printing results.");
			network.printResults();
		} catch (Exception e) {
			logger.error("Caught an unexpected exception.", e);
			System.exit(0);
		}
	}

}
