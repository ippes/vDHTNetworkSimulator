package net.tomp2p.vdht;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Class managing properties and providing (if required) default configuration.
 * 
 * @author Seppi
 */
public final class Configuration {

	private static Properties properties;

	// config file name
	private static final String configFile = "config.config";

	// config file attribute names
	private static final String RUNTIME_IN_MILLISECONDS = "runtimeInMilliseconds";
	private static final String PORT = "port";
	private static final String NUM_PEERS_MIN = "numPeersMin";
	private static final String NUM_PEERS_MAX = "numPeersMax";
	private static final String CHURN_RATE_JOIN = "churnRateJoin";
	private static final String CHURN_RATE_LEAVE = "churnRateLeave";
	private static final String CHURN_RATE_DELAY_IN_MILLISECONDS = "churnRateDelayInMilliseconds";
	private static final String CHURN_JOIN_LEAVE_RATE = "churnJoinLeaveRate";
	private static final String CHURN_STRATEGY_NAME = "churnStrategyName";
	private static final String NUM_KEYS = "numKeys";
	private static final String PUT_DELAY_MAX_IN_MILLISECONDS = "putDelayMaxInMilliseconds";
	private static final String PUT_DELAY_MIN_IN_MILLISECONDS = "putDelayMinInMilliseconds";

	private static void loadProperties() throws IOException {
		// create new properties
		properties = new Properties();

		FileInputStream in = null;
		try {
			// try to read config file
			in = new FileInputStream(configFile);
			// load config file
			properties.load(in);
		} catch (FileNotFoundException e) {
			// setup a default config file
			createDefaultConfig();
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	private static void createDefaultConfig() throws IOException {
		// create new config file
		new File(configFile).createNewFile();

		// set default values
		properties.setProperty(RUNTIME_IN_MILLISECONDS, "10000");
		properties.setProperty(PORT, "5000");
		properties.setProperty(NUM_PEERS_MIN, "800");
		properties.setProperty(NUM_PEERS_MAX, "1000");
		properties.setProperty(CHURN_RATE_JOIN, "10");
		properties.setProperty(CHURN_RATE_LEAVE, "10");
		properties.setProperty(CHURN_RATE_DELAY_IN_MILLISECONDS, "500");
		properties.setProperty(CHURN_JOIN_LEAVE_RATE, "0.5");
		properties.setProperty(NUM_KEYS, "100");
		properties.setProperty(PUT_DELAY_MAX_IN_MILLISECONDS, "2000");
		properties.setProperty(PUT_DELAY_MAX_IN_MILLISECONDS, "500");

		// store default config file
		FileOutputStream out = new FileOutputStream(configFile);
		properties.store(out, null);
	}

	/**
	 * Get value for RUNTIME_IN_MILLISECONDS from configuration.
	 * 
	 * @return runtime of the simulation in milliseconds
	 * @throws IOException
	 */
	public static int getRuntimeInMilliseconds() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(RUNTIME_IN_MILLISECONDS));
	}

	/**
	 * Get value for NUM_PEERS_MAX from configuration.
	 * 
	 * @return maximal allowed number of peers in the network
	 * @throws IOException
	 */
	public static int getNumPeersMax() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(NUM_PEERS_MAX));
	}

	/**
	 * Get value for NUM_PEERS_MIN from configuration.
	 * 
	 * @return minimal allowed number of peers in the network
	 * @throws IOException
	 */
	public static int getNumPeersMin() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(NUM_PEERS_MIN));
	}

	/**
	 * Get value for CHURN_RATE_JOIN from configuration.
	 * 
	 * @return number of peers which can join at once into the network
	 * @throws IOException
	 */
	public static int getChurnRateJoin() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(CHURN_RATE_JOIN));
	}

	/**
	 * Get value for CHURN_RATE_LEAVE from configuration.
	 * 
	 * @return number of peers which can leave at once the network
	 * @throws IOException
	 */
	public static int getChurnRateLeave() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(CHURN_RATE_LEAVE));
	}

	/**
	 * Get value for CHURN_RATE_DELAY_IN_MILLISECONDS from configuration.
	 * 
	 * @return delay in milliseconds between two churn events (join/leave)
	 * @throws IOException
	 */
	public static int getChurnRateDelayInMilliseconds() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(CHURN_RATE_DELAY_IN_MILLISECONDS));
	}

	/**
	 * Get value for CHURN_JOIN_LEAVE_RATE from configuration.
	 * 
	 * @return ratio between join and leave churn events
	 * @throws IOException
	 */
	public static double getChurnJoinLeaveRate() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Double.parseDouble(properties.getProperty(CHURN_JOIN_LEAVE_RATE));
	}

	/**
	 * Get value for CHURN_STRATEGY_NAME from configuration.
	 * 
	 * @return name of selected churn strategy
	 * @throws IOException
	 */
	public static String getChurnStrategyName() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return properties.getProperty(CHURN_STRATEGY_NAME);
	}

	/**
	 * Get value for PORT from configuration.
	 * 
	 * @return port number for the master peer of the network
	 * @throws IOException
	 */
	public static int getPort() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(PORT));
	}

	/**
	 * Get value for NUM_KEYS from configuration.
	 * 
	 * @return number of different keys which will do puts
	 * @throws IOException
	 */
	public static int getNumKeys() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(NUM_KEYS));
	}

	/**
	 * Get value for PUT_DELAY_MAX_IN_MILLISECONDS from configuration.
	 * 
	 * @return maximal delay between two put events
	 * @throws IOException
	 */
	public static int getPutDelayMaxInMilliseconds() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(PUT_DELAY_MAX_IN_MILLISECONDS));
	}

	/**
	 * Get value for PUT_DELAY_MIN_IN_MILLISECONDS from configuration.
	 * 
	 * @return minimal delay between two put events
	 * @throws IOException
	 */
	public static int getPutDelayMinInMilliseconds() throws IOException {
		if (properties == null) {
			loadProperties();
		}
		return Integer.parseInt(properties.getProperty(PUT_DELAY_MIN_IN_MILLISECONDS));
	}
}
