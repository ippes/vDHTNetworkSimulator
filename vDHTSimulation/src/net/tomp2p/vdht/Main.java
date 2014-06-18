package net.tomp2p.vdht;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws Exception {
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
	}

}
