package net.tomp2p.vdht;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Class managing properties.
 * 
 * @author Seppi
 */
public final class Configuration {

	/** config file attribute names **/

	// general settings
	private final String PORT = "port";
	private final String RUNTIME_IN_MILLISECONDS = "runtimeInMilliseconds";
	private final String NUM_PEERS_MIN = "numPeersMin";
	private final String NUM_PEERS_MAX = "numPeersMax";
	private final String TTL_CHECK_INTERVAL_IN_MILLISECONDS = "ttlCheckIntervalInMilliseconds";

	// replication settings
	private final String REPLICATION_STRATEGY_NAME = "replicationStrategyName";
	private final String REPLICATION_FACTOR = "replicationFactor";
	private final String REPLICATION_INTERVAL_IN_MILLISECONDS = "replicationIntervalInMilliseconds";

	// churn settings
	private final String CHURN_RATE_JOIN = "churnRateJoin";
	private final String CHURN_RATE_LEAVE = "churnRateLeave";
	private final String CHURN_RATE_MIN_DELAY_IN_MILLISECONDS = "churnRateMinDelayInMilliseconds";
	private final String CHURN_RATE_MAX_DELAY_IN_MILLISECONDS = "churnRateMaxDelayInMilliseconds";
	private final String CHURN_JOIN_LEAVE_RATE = "churnJoinLeaveRate";
	private final String CHURN_STRATEGY_NAME = "churnStrategyName";

	// put settings
	private final String NUM_PUTS = "numPuts";
	private final String MAX_VERSIONS = "maxVersions";
	private final String PUT_CONCURRENCY_FACTOR = "putConcurrencyFactor";
	private final String PUT_DELAY_MAX_IN_MILLISECONDS = "putDelayMaxInMilliseconds";
	private final String PUT_DELAY_MIN_IN_MILLISECONDS = "putDelayMinInMilliseconds";
	private final String PUT_STRATEGY_NAME = "putStrategyName";
	private final String PUT_TTL_IN_SECONDS = "putTTLInSeconds";
	private final String PUT_PREPARE_TTL_IN_SECONDS = "putPrepareTTLInSeconds";

	private final Properties properties = new Properties();

	public Configuration(File configFile) throws IOException {
		FileInputStream in = null;
		try {
			// try to read config file
			in = new FileInputStream(configFile);
			// load config file
			properties.load(in);
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	/**
	 * Get value for MAX_VERSIONS from configuration.
	 * 
	 * @return maximal amount of versions for one key
	 */
	public int getMaxVersions() {
		return Integer.parseInt(properties.getProperty(MAX_VERSIONS));
	}

	/**
	 * Get value for RUNTIME_IN_MILLISECONDS from configuration.
	 * 
	 * @return runtime of the simulation in milliseconds
	 */
	public int getRuntimeInMilliseconds() {
		return Integer
				.parseInt(properties.getProperty(RUNTIME_IN_MILLISECONDS));
	}

	/**
	 * Get value for NUM_PEERS_MAX from configuration.
	 * 
	 * @return maximal allowed number of peers in the network
	 */
	public int getNumPeersMax() {
		return Integer.parseInt(properties.getProperty(NUM_PEERS_MAX));
	}

	/**
	 * Get value for NUM_PEERS_MIN from configuration.
	 * 
	 * @return minimal allowed number of peers in the network
	 */
	public int getNumPeersMin() {
		return Integer.parseInt(properties.getProperty(NUM_PEERS_MIN));
	}

	/**
	 * Get value for CHURN_RATE_JOIN from configuration.
	 * 
	 * @return number of peers which can join at once into the network
	 */
	public int getChurnRateJoin() {
		return Integer.parseInt(properties.getProperty(CHURN_RATE_JOIN));
	}

	/**
	 * Get value for CHURN_RATE_LEAVE from configuration.
	 * 
	 * @return number of peers which can leave at once the network
	 */
	public int getChurnRateLeave() {
		return Integer.parseInt(properties.getProperty(CHURN_RATE_LEAVE));
	}

	/**
	 * Get value for CHURN_RATE_MIN_DELAY_IN_MILLISECONDS from configuration.
	 * 
	 * @return minimum delay in milliseconds between two churn events
	 *         (join/leave)
	 */
	public int getChurnRateMinDelayInMilliseconds() {
		return Integer.parseInt(properties
				.getProperty(CHURN_RATE_MIN_DELAY_IN_MILLISECONDS));
	}

	/**
	 * Get value for CHURN_RATE_MAX_DELAY_IN_MILLISECONDS from configuration.
	 * 
	 * @return maximal delay in milliseconds between two churn events
	 *         (join/leave)
	 */
	public int getChurnRateMaxDelayInMilliseconds() {
		return Integer.parseInt(properties
				.getProperty(CHURN_RATE_MAX_DELAY_IN_MILLISECONDS));
	}

	/**
	 * Get value for CHURN_JOIN_LEAVE_RATE from configuration.
	 * 
	 * @return ratio between join and leave churn events
	 */
	public double getChurnJoinLeaveRate() {
		return Double
				.parseDouble(properties.getProperty(CHURN_JOIN_LEAVE_RATE));
	}

	/**
	 * Get value for CHURN_STRATEGY_NAME from configuration.
	 * 
	 * @return name of selected churn strategy <code>off</code>,
	 *         <code>stepwise</code>, <code>stepwiseRandom</code> or
	 *         <code>wild</code>
	 */
	public String getChurnStrategyName() {
		return properties.getProperty(CHURN_STRATEGY_NAME);
	}

	/**
	 * Get value for PORT from configuration.
	 * 
	 * @return port number for the master peer of the network
	 */
	public int getPort() {
		return Integer.parseInt(properties.getProperty(PORT));
	}

	/**
	 * Get value for NUM_PUTS from configuration.
	 * 
	 * @return how often a key has to be put
	 */
	public int getNumPuts() {
		return Integer.parseInt(properties.getProperty(NUM_PUTS));
	}

	/**
	 * Get value for PUT_DELAY_MAX_IN_MILLISECONDS from configuration.
	 * 
	 * @return maximal delay between two put events
	 */
	public int getPutDelayMaxInMilliseconds() {
		return Integer.parseInt(properties
				.getProperty(PUT_DELAY_MAX_IN_MILLISECONDS));
	}

	/**
	 * Get value for PUT_DELAY_MIN_IN_MILLISECONDS from configuration.
	 * 
	 * @return minimal delay between two put events
	 */
	public int getPutDelayMinInMilliseconds() {
		return Integer.parseInt(properties
				.getProperty(PUT_DELAY_MIN_IN_MILLISECONDS));
	}

	/**
	 * Get value for PUT_CONCURRENCY_FACTOR from configuration.
	 * 
	 * @return number of peers putting with the same key
	 */
	public int getPutConcurrencyFactor() {
		return Integer.parseInt(properties.getProperty(PUT_CONCURRENCY_FACTOR));
	}

	/**
	 * Get value for REPLICATION_STRATEGY_NAME from configuration.
	 * 
	 * @return type of replication <code>off</code>, <code>root</code> or
	 *         <code>nRoot</code>
	 */
	public String getReplicationStrategyName() {
		return properties.getProperty(REPLICATION_STRATEGY_NAME);
	}

	/**
	 * Get value for PUT_STRATEGY_NAME from configuration.
	 * 
	 * @return putting approach <code>traditional</code>,
	 *         <code>optimistic</code> or <code>pessimistic</code>
	 */
	public String getPutStrategyName() {
		return properties.getProperty(PUT_STRATEGY_NAME);
	}

	/**
	 * Get value for REPLICATION_INTERVAL_IN_MILLISECONDS from configuration.
	 * 
	 * @return interval to trigger the replication in milliseconds
	 */
	public int getReplicationIntervalInMilliseconds() {
		return Integer.parseInt(properties
				.getProperty(REPLICATION_INTERVAL_IN_MILLISECONDS));
	}

	/**
	 * Get value for TTL_CHECK_INTERVAL_IN_MILLISECONDS from configuration.
	 * 
	 * @return frequency to check storage for expired data
	 */
	public int getTTLCheckIntervalInMilliseconds() {
		return Integer.parseInt(properties
				.getProperty(TTL_CHECK_INTERVAL_IN_MILLISECONDS));
	}

	/**
	 * Get value for REPLICATION_FACTOR from configuration.
	 * 
	 * @return size of replica set
	 */
	public int getReplicationFactor() {
		return Integer.parseInt(properties.getProperty(REPLICATION_FACTOR));
	}

	/**
	 * Get value for PUT_TTL_IN_SECONDS from configuration.
	 * 
	 * @return time to live of a stored (and confirmed) object
	 */
	public int getPutTTLInSeconds() {
		return Integer.parseInt(properties.getProperty(PUT_TTL_IN_SECONDS));
	}

	/**
	 * Get value for PUT_PREPARE_TTL_IN_SECONDS from configuration.
	 * 
	 * @return time to live of a stored but not not confirmed (prepared) object
	 */
	public int getPutPrepareTTLInSeconds() {
		return Integer.parseInt(properties
				.getProperty(PUT_PREPARE_TTL_IN_SECONDS));
	}

}
