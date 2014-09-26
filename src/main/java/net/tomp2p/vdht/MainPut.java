package net.tomp2p.vdht;

import java.io.File;
import java.io.IOException;

import net.tomp2p.vdht.simulator.PutSimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes and runs network simulations according given configuration files.
 * Starts churn. Starts putting procedure. Waits a certain amount and stops
 * afterwards the simulation or waits till simulation finishes. Shutdowns churn.
 * Stores results. Shutdowns network.
 * 
 * @author Seppi
 */
public class MainPut {

	private static Logger logger = LoggerFactory.getLogger(MainPut.class);

	public static void main(String[] args) {
		if (args.length != 1 && args.length != 3) {
			logger.info("Config file is missing.");
			return;
		}

		Thread.currentThread().setName("vDHT - Main Put");

		// load config file
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
				logger.info("========================================================");
				return;
			}
			PutSimulator simulator = new PutSimulator(configuration);

			logger.info("Setting up the network simulator.");
			if (args.length == 3) {
				// create network and bootstrap to given node
				simulator.createNetwork(args[1], Integer.parseInt(args[2]));
			} else {
				simulator.createNetwork();
			}

			logger.info("Start simulating churn.");
			simulator.startChurn();

			int runtimeInMilliseconds = configuration
					.getRuntimeInMilliseconds();
			int numPuts = configuration.getNumPuts();
			// run according given runtime
			if (runtimeInMilliseconds > 0 && numPuts < 0) {
				logger.info("Start putting data.");
				simulator.startPutting();
				logger.info("Running simulation for {} milliseconds.",
						runtimeInMilliseconds);
				Thread.sleep(runtimeInMilliseconds);
			} else if (runtimeInMilliseconds < 0 && numPuts > 0) {
				logger.info("Start putting data.");
				simulator.startPutting();
				// run till simulation stops
				while (simulator.isPuttingRunning()) {
					Thread.sleep(500);
				}
			}

			simulator.shutDownChurn();
			logger.info("Churn stopped.");

			simulator.shutDownPutCoordinators();
			logger.info("Putting stopped.");

			simulator.loadAndStoreResults();
			logger.info("Results loaded and stored.");

			simulator.shutDownNetwork();
			logger.info("Network is down.");

			if (numPuts > 0 || runtimeInMilliseconds > 0) {
				logger.debug("Printing results.");
				simulator.printResults();
			}

			logger.info("========================================================");
		} catch (Exception e) {
			logger.error("Caught an unexpected exception.", e);
			System.exit(0);
		}
	}

}
