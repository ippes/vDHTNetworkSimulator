package net.tomp2p.vdht;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.replication.IndirectReplication;
import net.tomp2p.vdht.churn.ChurnExecutor;
import net.tomp2p.vdht.churn.ChurnStrategy;
import net.tomp2p.vdht.put.PutCoordinator;
import net.tomp2p.vdht.put.PutStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A network simulator setting up a local peer-to-peer network. Churn can be
 * enabled. All configuration is fetched from {@link Configuration} class.
 * 
 * @author Seppi
 */
public class LocalNetworkSimulator {

	private static Logger logger = LoggerFactory
			.getLogger(LocalNetworkSimulator.class);

	private PeerDHT masterPeer;
	private final List<PeerDHT> peers = new ArrayList<PeerDHT>();

	private ChurnExecutor churnExecutor;
	private PutCoordinator putCoordinator;

	private final Random random = new Random();
	private final KeyLock<Number160> keyLock = new KeyLock<Number160>();

	private final Configuration configuration;

	public LocalNetworkSimulator(Configuration configuration) {
		this.configuration = configuration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void createNetwork() throws IOException {
		// initially create peers within given boundaries
		int numPeers = (configuration.getNumPeersMax() + configuration
				.getNumPeersMin()) / 2;
		for (int i = 0; i < numPeers; i++) {
			if (i == 0) {
				// create master peer
				masterPeer = createPeer(true);

				// enable replication if required
				enableReplication(masterPeer);

				if (!configuration.getBootstrapIP().equals("local")
						&& configuration.getBootstrapPort() > 0) {
					FutureDiscover futureDiscover = masterPeer
							.peer()
							.discover()
							.inetAddress(
									Inet4Address.getByName(configuration
											.getBootstrapIP()))
							.ports(configuration.getBootstrapPort()).start();
					futureDiscover.awaitUninterruptibly();

					if (futureDiscover.isSuccess()) {
						logger.debug(
								"Discovering successful. Outside address is '{}'.",
								futureDiscover.peerAddress());
					} else {
						logger.warn("Discovering failed: {}.",
								futureDiscover.failedReason());
						throw new IllegalStateException("Discovering failed.");
					}

					FutureBootstrap futureBootstrap = masterPeer
							.peer()
							.bootstrap()
							.inetAddress(
									Inet4Address.getByName(configuration
											.getBootstrapIP()))
							.ports(configuration.getBootstrapPort()).start();
					futureBootstrap.awaitUninterruptibly();

					if (futureBootstrap.isSuccess()) {
						logger.debug(
								"Bootstrapping successful. Bootstrapped to '{}'.",
								configuration.getBootstrapIP());
					} else {
						logger.warn("Bootstrapping failed: {}.",
								futureBootstrap.failedReason());
						throw new IllegalStateException("Bootstraping failed.");
					}
				}

				logger.trace("Master Peer added to network. peer id = '{}'",
						masterPeer.peerID());
			} else {
				// create peer
				PeerDHT peer = createPeer(false);

				// enable replication if required
				enableReplication(peer);

				// bootstrap to master peer
				FutureBootstrap fBoo = peer.peer().bootstrap()
						.peerAddress(masterPeer.peerAddress()).start();
				fBoo.awaitUninterruptibly();

				peers.add(peer);
				logger.trace("Peer added to network. peer id = '{}'",
						peer.peerID());
			}
		}
		logger.debug("Network created. numPeers = '{}'", numPeers);
	}

	private PeerDHT createPeer(boolean isMaster) throws IOException {
		// create a new peer id
		Number160 peerId = new Number160(random);
		// disable peer verification (faster mutual acceptance)
		PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(
				peerId);
		peerMapConfiguration.peerVerification(false);
		PeerMap peerMap = new PeerMap(peerMapConfiguration);
		// reduce TCP number of short-lived TCP connections to avoid timeouts
		ChannelClientConfiguration channelConfig = PeerBuilder
				.createDefaultChannelClientConfiguration();
		channelConfig.maxPermitsTCP(10);

		return new PeerBuilderDHT(new PeerBuilder(peerId)
				.ports(configuration.getPort()).peerMap(peerMap)
				.masterPeer(isMaster ? null : masterPeer.peer())
				.channelClientConfiguration(channelConfig).start()).storage(
				new StorageMemory(configuration
						.getTTLCheckIntervalInMilliseconds(), configuration
						.getMaxVersions())).start();
	}

	private void enableReplication(PeerDHT peer) {
		switch (configuration.getReplication()) {
		case "off":
			// don't enable any replication
			break;
		case "0Root":
			// set replication interval, start replication with 0-root approach
			new IndirectReplication(peer)
					.intervalMillis(
							configuration
									.getReplicationIntervalInMilliseconds())
					.replicationFactor(configuration.getReplicationFactor())
					.start();
			break;
		case "nRoot":
		default:
			// set replication interval, start replication with n-root approach
			new IndirectReplication(peer)
					.intervalMillis(
							configuration
									.getReplicationIntervalInMilliseconds())
					.nRoot()
					.replicationFactor(configuration.getReplicationFactor())
					.start();
		}
	}

	public void startChurn() {
		if (configuration.getChurnStrategyName().equals("off")) {
			logger.debug("No churn enabled.");
		} else {
			churnExecutor = new ChurnExecutor(this);
			churnExecutor.start();
			logger.debug("Churn started.");
		}
	}

	public void startPutting() {
		putCoordinator = new PutCoordinator(this);
		putCoordinator.start();
		logger.debug("Putting started.");
	}

	public boolean isPuttingRunning() {
		return !putCoordinator.isShutDown();
	}

	public boolean isChurnRunning() {
		return !churnExecutor.isShutdown();
	}

	public void shutDownChurn() throws InterruptedException {
		if (churnExecutor != null) {
			churnExecutor.shutdown();
			while (isChurnRunning()) {
				Thread.sleep(100);
			}
		}
	}

	public void shutDownPutCoordinators() throws InterruptedException {
		if (putCoordinator != null) {
			putCoordinator.shutdown();
			while (isPuttingRunning()) {
				Thread.sleep(100);
			}
		}
	}

	public void shutDownNetwork() {
		for (PeerDHT peer : peers) {
			peer.shutdown().awaitUninterruptibly();
		}
		peers.clear();
		if (masterPeer != null) {
			masterPeer.shutdown().awaitUninterruptibly();
		}
		masterPeer = null;
	}

	public void loadAndStoreResults() {
		// load latest version
		putCoordinator.loadResults();
		// store settings and results in a file
		Outcome.writeResult(configuration, putCoordinator.getResult());
	}

	public void printResults() {
		putCoordinator.getResult().printResults();
	}

	public void addPeersToTheNetwork(ChurnStrategy churnStrategy) {
		int numberOfPeerToJoin = churnStrategy.getNumJoiningPeers(peers.size());
		logger.debug("Joining {} peers. # peer = '{}'", numberOfPeerToJoin,
				peers.size() + 1);
		for (int i = 0; i < numberOfPeerToJoin; i++) {
			try {
				// create new peer to join
				PeerDHT newPeer = createPeer(false);

				// enable replication if required
				enableReplication(newPeer);

				// bootstrap to master peer
				FutureBootstrap futureBootstrap = newPeer.peer().bootstrap()
						.peerAddress(masterPeer.peerAddress()).start();
				futureBootstrap.awaitUninterruptibly();

				peers.add(newPeer);
				logger.trace("New peer joined the network. peer id = '{}'",
						newPeer.peerID());
			} catch (IOException e) {
				logger.error("Couldn't create a new peer.", e);
			}
		}
	}

	public void removePeersFromNetwork(ChurnStrategy churnStrategy) {
		int numberOfLeavingPeers = churnStrategy.getNumLeavingPeers(peers
				.size());
		logger.debug("Leaving {} peers. # peers = '{}'", numberOfLeavingPeers,
				peers.size() + 1);
		for (int i = 0; i < numberOfLeavingPeers; i++) {
			KeyLock<Number160>.RefCounterLock lock = null;
			PeerDHT peer = null;
			while (lock == null) {
				peer = peers.get(random.nextInt(peers.size()));
				lock = keyLock.tryLock(peer.peerID());
			}
			peers.remove(peer);
			peer.shutdown().awaitUninterruptibly();
			keyLock.unlock(lock);
			logger.trace("Peer leaved the network. peer id = '{}'",
					peer.peerID());
		}
	}

	public void put(PutStrategy putStrategy) throws Exception {
		KeyLock<Number160>.RefCounterLock lock = null;
		PeerDHT peer = null;
		while (lock == null) {
			peer = peers.get(random.nextInt(peers.size()));
			lock = keyLock.tryLock(peer.peerID());
		}
		try {
			putStrategy.getUpdateAndPut(peer);
		} finally {
			keyLock.unlock(lock);
		}
	}

	@SuppressWarnings("unchecked")
	public Map<String, Integer> get(Number480 key) {
		KeyLock<Number160>.RefCounterLock lock = null;
		PeerDHT peer = null;
		while (lock == null) {
			peer = peers.get(random.nextInt(peers.size()));
			lock = keyLock.tryLock(peer.peerID());
		}
		try {
			// load latest string
			FutureGet futureGet = peer.get(key.locationKey())
					.contentKey(key.contentKey()).domainKey(key.domainKey())
					.getLatest().start();
			futureGet.awaitUninterruptibly();

			return (Map<String, Integer>) futureGet.data().object();
		} catch (Exception e) {
			logger.error("Couldn't get result.", e);
			return null;
		} finally {
			keyLock.unlock(lock);
		}
	}

}
