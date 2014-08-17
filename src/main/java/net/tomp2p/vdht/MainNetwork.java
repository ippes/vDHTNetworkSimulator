package net.tomp2p.vdht;

import java.io.File;
import java.io.IOException;
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
public class MainNetwork {

	private static Logger logger = LoggerFactory.getLogger(MainNetwork.class);

	public static void main(String[] args) {
		Thread.currentThread().setName("vDHT - Main");

		List<File> configFiles = loadConfigFiles(args);

		for (File configFile : configFiles) {
			try {
				logger.info("========================================================");
				logger.info("Running network simulation. config file = '{}'",
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
				if (runtimeInMilliseconds > 0) {
					Thread.sleep(runtimeInMilliseconds);
				} else {
					Thread.sleep(Long.MAX_VALUE);
				}

				network.shutDownChurn();
				logger.info("Churn stopped.");

				network.shutDownNetwork();
				logger.info("Network is down.");

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
