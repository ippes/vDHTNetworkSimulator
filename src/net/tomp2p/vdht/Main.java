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

			logger.debug("Setting up the network simulator.");
			network.createNetwork();

			logger.debug("Start simulating churn.");
			network.startChurn();

			logger.debug("Start putting data.");
			network.startPutting();

			int runtimeInMilliseconds = Configuration.getRuntimeInMilliseconds();
			logger.debug("Running simulation for {} milliseconds.", runtimeInMilliseconds);
			Thread.sleep(runtimeInMilliseconds);

			logger.debug("Stopping network simulator.");
			network.shutDownNetwork();
		} catch (Exception e) {
			logger.error("Caught an unexpected exception.", e);
			System.exit(0);
		}
	}

}
