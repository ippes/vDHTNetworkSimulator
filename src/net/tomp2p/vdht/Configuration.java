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
	private final String BOOTSTRAP_IP = "bootstrapIP";
	private final String BOOTSTRAP_PORT = "bootstrapPort";
	private final String RUNTIME_IN_MILLISECONDS = "runtimeInMilliseconds";
	private final String NUM_PEERS_MIN = "numPeersMin";
	private final String NUM_PEERS_MAX = "numPeersMax";
	private final String TTL_CHECK_INTERVAL_IN_MILLISECONDS = "ttlCheckIntervalInMilliseconds";

	// replication settings
	private final String REPLICATION = "replication";
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
	private final String NUM_KEYS = "numKeys";
	private final String NUM_PUTS = "numPuts";
	private final String MAX_VERSIONS = "maxVersions";
	private final String PUT_CONCURRENCY_FACTOR = "putConcurrencyFactor";
	private final String PUT_DELAY_MAX_IN_MILLISECONDS = "putDelayMaxInMilliseconds";
	private final String PUT_DELAY_MIN_IN_MILLISECONDS = "putDelayMinInMilliseconds";
	private final String PUT_STRATEGY_NAME = "putStrategyName";
	private final String PUT_TTL_IN_SECONDS = "putTTLInSeconds";
	private final String PUT_PREPARE_TTL_IN_SECONDS = "putPrepareTTLInSeconds";

	/** config attributes **/

	// general settings
	private final int port;
	private final String bootstrapIP;
	private final int bootstrapPort;
	private final int runtimeInMilliseconds;
	private final int numPeersMin;
	private final int numPeersMax;
	private final int ttlCheckIntervalInMilliseconds;

	// replication settings
	private final String replication;
	private final int replicationFactor;
	private final int replicationIntervalInMilliseconds;

	// churn settings
	private final int churnRateJoin;
	private final int churnRateLeave;
	private final int churnRateMinDelayInMilliseconds;
	private final int churnRateMaxDelayInMilliseconds;
	private final double churnJoinLeaveRate;
	private final String churnStrategyName;

	// put settings
	private final int numKeys;
	private final int numPuts;
	private final int maxVersions;
	private final int putConcurrencyFactor;
	private final int putDelayMaxInMilliseconds;
	private final int putDelayMinInMilliseconds;
	private final String putStrategyName;
	private final int putTTLInSeconds;
	private final int putPrepareTTLInSeconds;

	public Configuration(File configFile) throws IOException {
		// create new properties
		Properties properties = new Properties();

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

		// load general settings
		this.port = Integer.parseInt(properties.getProperty(PORT));
		this.bootstrapIP = properties.getProperty(BOOTSTRAP_IP);
		this.bootstrapPort = Integer.parseInt(properties.getProperty(BOOTSTRAP_PORT));
		this.runtimeInMilliseconds = Integer.parseInt(properties.getProperty(RUNTIME_IN_MILLISECONDS));
		this.numPeersMin = Integer.parseInt(properties.getProperty(NUM_PEERS_MIN));
		this.numPeersMax = Integer.parseInt(properties.getProperty(NUM_PEERS_MAX));
		this.ttlCheckIntervalInMilliseconds = Integer.parseInt(properties
				.getProperty(TTL_CHECK_INTERVAL_IN_MILLISECONDS));

		// load replication settings
		this.replication = properties.getProperty(REPLICATION);
		this.replicationFactor = Integer.parseInt(properties.getProperty(REPLICATION_FACTOR));
		this.replicationIntervalInMilliseconds = Integer.parseInt(properties
				.getProperty(REPLICATION_INTERVAL_IN_MILLISECONDS));

		// load churn settings
		this.churnRateJoin = Integer.parseInt(properties.getProperty(CHURN_RATE_JOIN));
		this.churnRateLeave = Integer.parseInt(properties.getProperty(CHURN_RATE_LEAVE));
		this.churnRateMinDelayInMilliseconds = Integer.parseInt(properties
				.getProperty(CHURN_RATE_MIN_DELAY_IN_MILLISECONDS));
		this.churnRateMaxDelayInMilliseconds = Integer.parseInt(properties
				.getProperty(CHURN_RATE_MAX_DELAY_IN_MILLISECONDS));
		this.churnJoinLeaveRate = Double.parseDouble(properties.getProperty(CHURN_JOIN_LEAVE_RATE));
		this.churnStrategyName = properties.getProperty(CHURN_STRATEGY_NAME);

		// load put settings
		this.numKeys = Integer.parseInt(properties.getProperty(NUM_KEYS));
		this.numPuts = Integer.parseInt(properties.getProperty(NUM_PUTS));
		this.maxVersions = Integer.parseInt(properties.getProperty(MAX_VERSIONS));
		this.putConcurrencyFactor = Integer.parseInt(properties.getProperty(PUT_CONCURRENCY_FACTOR));
		this.putDelayMaxInMilliseconds = Integer.parseInt(properties
				.getProperty(PUT_DELAY_MAX_IN_MILLISECONDS));
		this.putDelayMinInMilliseconds = Integer.parseInt(properties
				.getProperty(PUT_DELAY_MIN_IN_MILLISECONDS));
		this.putStrategyName = properties.getProperty(PUT_STRATEGY_NAME);
		this.putTTLInSeconds = Integer.parseInt(properties.getProperty(PUT_TTL_IN_SECONDS));
		this.putPrepareTTLInSeconds = Integer.parseInt(properties.getProperty(PUT_PREPARE_TTL_IN_SECONDS));
	}

	/**
	 * Get value for MAX_VERSIONS from configuration.
	 * 
	 * @return maximal amount of versions for one key
	 */
	public int getMaxVersions() {
		return maxVersions;
	}

	/**
	 * Get value for RUNTIME_IN_MILLISECONDS from configuration.
	 * 
	 * @return runtime of the simulation in milliseconds
	 */
	public int getRuntimeInMilliseconds() {
		return runtimeInMilliseconds;
	}

	/**
	 * Get value for NUM_PEERS_MAX from configuration.
	 * 
	 * @return maximal allowed number of peers in the network
	 */
	public int getNumPeersMax() {
		return numPeersMax;
	}

	/**
	 * Get value for NUM_PEERS_MIN from configuration.
	 * 
	 * @return minimal allowed number of peers in the network
	 */
	public int getNumPeersMin() {
		return numPeersMin;
	}

	/**
	 * Get value for CHURN_RATE_JOIN from configuration.
	 * 
	 * @return number of peers which can join at once into the network
	 */
	public int getChurnRateJoin() {
		return churnRateJoin;
	}

	/**
	 * Get value for CHURN_RATE_LEAVE from configuration.
	 * 
	 * @return number of peers which can leave at once the network
	 */
	public int getChurnRateLeave() {
		return churnRateLeave;
	}

	/**
	 * Get value for CHURN_RATE_MIN_DELAY_IN_MILLISECONDS from configuration.
	 * 
	 * @return minimum delay in milliseconds between two churn events
	 *         (join/leave)
	 */
	public int getChurnRateMinDelayInMilliseconds() {
		return churnRateMinDelayInMilliseconds;
	}

	/**
	 * Get value for CHURN_RATE_MAX_DELAY_IN_MILLISECONDS from configuration.
	 * 
	 * @return maximal delay in milliseconds between two churn events
	 *         (join/leave)
	 */
	public int getChurnRateMaxDelayInMilliseconds() {
		return churnRateMaxDelayInMilliseconds;
	}

	/**
	 * Get value for CHURN_JOIN_LEAVE_RATE from configuration.
	 * 
	 * @return ratio between join and leave churn events
	 */
	public double getChurnJoinLeaveRate() {
		return churnJoinLeaveRate;
	}

	/**
	 * Get value for CHURN_STRATEGY_NAME from configuration.
	 * 
	 * @return name of selected churn strategy <code>off</code>, <code>stepwise</code>,
	 *         <code>stepwiseRandom</code> or <code>wild</code>
	 */
	public String getChurnStrategyName() {
		return churnStrategyName;
	}

	/**
	 * Get value for PORT from configuration.
	 * 
	 * @return port number for the master peer of the network
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Get value for BOOTSTRAP_IP from configuration.
	 * 
	 * @return ip address of the bootstrap node
	 */
	public String getBootstrapIP() {
		return bootstrapIP;
	}

	/**
	 * Get value for BOOTSTRAP_PORT from configuration.
	 * 
	 * @return port number of the bootstrap node
	 */
	public int getBootstrapPort() {
		return bootstrapPort;
	}

	/**
	 * Get value for NUM_KEYS from configuration.
	 * 
	 * @return number of different keys which will do puts
	 */
	public int getNumKeys() {
		return numKeys;
	}

	/**
	 * Get value for NUM_PUTS from configuration.
	 * 
	 * @return how often a key has to be put
	 */
	public int getNumPuts() {
		return numPuts;
	}

	/**
	 * Get value for PUT_DELAY_MAX_IN_MILLISECONDS from configuration.
	 * 
	 * @return maximal delay between two put events
	 */
	public int getPutDelayMaxInMilliseconds() {
		return putDelayMaxInMilliseconds;
	}

	/**
	 * Get value for PUT_DELAY_MIN_IN_MILLISECONDS from configuration.
	 * 
	 * @return minimal delay between two put events
	 */
	public int getPutDelayMinInMilliseconds() {
		return putDelayMinInMilliseconds;
	}

	/**
	 * Get value for PUT_CONCURRENCY_FACTOR from configuration.
	 * 
	 * @return number of peers putting with the same key
	 */
	public int getPutConcurrencyFactor() {
		return putConcurrencyFactor;
	}

	/**
	 * Get value for REPLICATION from configuration.
	 * 
	 * @return type of replication <code>off</code>, <code>root</code> or <code>nRoot</code>
	 */
	public String getReplication() {
		return replication;
	}

	/**
	 * Get value for PUT_STRATEGY_NAME from configuration.
	 * 
	 * @return putting approach <code>traditional</code>, <code>optimistic</code> or <code>pessimistic</code>
	 */
	public String getPutStrategyName() {
		return putStrategyName;
	}

	/**
	 * Get value for REPLICATION_INTERVAL_IN_MILLISECONDS from configuration.
	 * 
	 * @return interval to trigger the replication in milliseconds
	 */
	public int getReplicationIntervalInMilliseconds() {
		return replicationIntervalInMilliseconds;
	}

	/**
	 * Get value for TTL_CHECK_INTERVAL_IN_MILLISECONDS from configuration.
	 * 
	 * @return frequency to check storage for expired data
	 */
	public int getTTLCheckIntervalInMilliseconds() {
		return ttlCheckIntervalInMilliseconds;
	}

	/**
	 * Get value for REPLICATION_FACTOR from configuration.
	 * 
	 * @return size of replica set
	 */
	public int getReplicationFactor() {
		return replicationFactor;
	}

	/**
	 * Get value for PUT_TTL_IN_SECONDS from configuration.
	 * 
	 * @return time to live of a stored (and confirmed) object
	 */
	public int getPutTTLInSeconds() {
		return putTTLInSeconds;
	}

	/**
	 * Get value for PUT_PREPARE_TTL_IN_SECONDS from configuration.
	 * 
	 * @return time to live of a stored but not not confirmed (prepared) object
	 */
	public int getPutPrepareTTLInSeconds() {
		return putPrepareTTLInSeconds;
	}

}
