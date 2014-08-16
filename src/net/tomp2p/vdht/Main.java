package net.tomp2p.vdht;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes and runs network simulations according given configuration files.
 * Starts churn. Starts putting procedure. Waits a certain amount so that
 * simulation can run. Stops simulation and shutdowns network.
 * 
 * @author Seppi
 */
public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		Thread.currentThread().setName("vDHT - Main");

		List<File> configFiles = loadConfigFiles(args);

		for (File configFile : configFiles) {
			try {
				logger.info("========================================================");
				logger.info("Running simulation. config file = '{}'",
						configFile.getName());

				// initialize simulation
				Configuration configuration;
				try {
					configuration = new Configuration(configFile);
				} catch (IOException e) {
					logger.error("Couldn't read given config file = '{}'",
							configFile.getName());
					continue;
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
				} else {
					logger.info("Type 'stop' to shutdown network.");
					String input;
					while (true) {
						BufferedReader bufferRead = new BufferedReader(
								new InputStreamReader(System.in));
						input = bufferRead.readLine();
						if (input.equals("stop")) {
							logger.info("Stopping.");
							break;
						} else {
							logger.info("Unknown command. '{}'", input);
						}
					}
				}

				network.shutDownChurn();
				logger.info("Churn stopped.");

				network.shutDownPutCoordinators();
				logger.info("Putting stopped.");

				network.loadAndStoreResults();
				logger.info("Results loaded and stored.");

				network.shutDownNetwork();
				logger.info("Network is down.");

				logger.debug("Printing results.");
				network.printResults();

				logger.info("========================================================");
			} catch (Exception e) {
				logger.error("Caught an unexpected exception.", e);
				System.exit(0);
			}
		}
	}

	private static List<File> loadConfigFiles(String[] args) {
		List<File> configFiles = new ArrayList<File>();
		if (args.length == 0) {
			for (File file : new File(".").listFiles()) {
				if (file.getName().endsWith(".config")) {
					configFiles.add(file);
				}
			}
		} else {
			for (String configFileName : args) {
				configFiles.add(new File(configFileName));
			}
		}
		return configFiles;
	}

}
