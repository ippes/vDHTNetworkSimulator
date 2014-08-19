package net.tomp2p.vdht;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes and runs network simulations according given configuration files.
 * Starts churn. Starts putting procedure. Waits a certain amount so that
 * simulation can run. Stops simulation and shutdowns network.
 * 
 * @author Seppi
 */
public class MainPut {

	private static Logger logger = LoggerFactory.getLogger(MainPut.class);

	public static void main(String[] args) {
		Thread.currentThread().setName("vDHT - Main Put");

		File configFile = new File(args[0]);
		try {
			logger.info("========================================================");
			logger.info("Running putting simulation. config file = '{}'",
					configFile.getName());

			// initialize simulation
			Configuration configuration = null;
			try {
				configuration = new Configuration(configFile);
			} catch (IOException e) {
				logger.error("Couldn't read given config file = '{}'",
						configFile.getName());
			}
			LocalNetworkSimulator network = new LocalNetworkSimulator(
					configuration);

			logger.info("Setting up the network simulator.");
			network.createNetwork();

			logger.info("Start simulating churn.");
			network.startChurn();

			int runtimeInMilliseconds = configuration
					.getRuntimeInMilliseconds();
			int numPuts = configuration.getNumPuts();
			// run according given runtime
			if (runtimeInMilliseconds > 0 && numPuts < 0) {
				logger.info("Start putting data.");
				network.startPutting();
				logger.info("Running simulation for {} milliseconds.",
						runtimeInMilliseconds);
				Thread.sleep(runtimeInMilliseconds);
			} else if (runtimeInMilliseconds < 0 && numPuts > 0) {
				logger.info("Start putting data.");
				network.startPutting();
				// run till simulation stops
				while (network.isPuttingRunning()) {
					Thread.sleep(500);
				}
			}

			network.shutDownChurn();
			logger.info("Churn stopped.");

			network.shutDownPutCoordinators();
			logger.info("Putting stopped.");

			network.loadAndStoreResults();
			logger.info("Results loaded and stored.");

			network.deleteData();
			logger.info("Data removed.");

			network.shutDownNetwork();
			logger.info("Network is down.");

			if (numPuts > 0 || runtimeInMilliseconds > 0) {
				logger.debug("Printing results.");
				network.printResults();
			}

			logger.info("========================================================");
		} catch (Exception e) {
			logger.error("Caught an unexpected exception.", e);
			System.exit(0);
		}
	}

}
