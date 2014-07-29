package net.tomp2p.vdht;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.replication.IndirectReplication;
import net.tomp2p.vdht.churn.ChurnStrategy;
import net.tomp2p.vdht.churn.StepwiseChurnStrategy;
import net.tomp2p.vdht.churn.StepwiseRandomChurnStrategy;
import net.tomp2p.vdht.churn.WildChurnStrategy;
import net.tomp2p.vdht.put.OptimisticPutStrategy;
import net.tomp2p.vdht.put.PesimisticPutStrategy;
import net.tomp2p.vdht.put.PutStrategy;
import net.tomp2p.vdht.put.TraditionalPutStrategy;
import net.tomp2p.vdht.put.TraditionalVersionPutStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A network simulator setting up a local peer-to-peer network. Churn can be
 * enabled. All configuration is fetched from {@link Configuration} class.
 * 
 * @author Seppi
 */
public class LocalNetworkSimulator {

	private static Logger logger = LoggerFactory.getLogger(LocalNetworkSimulator.class);

	private PeerDHT masterPeer;
	private final List<PeerDHT> peers = new ArrayList<PeerDHT>();

	private ChurnExecutor churnExecutor;
	private PutCoordinator[] putCoordinators;

	private final Random random = new Random();
	private final KeyLock<Number160> keyLock = new KeyLock<Number160>();

	private final Configuration configuration;

	public LocalNetworkSimulator(Configuration configuration) {
		this.configuration = configuration;
	}

	public void createNetwork() throws IOException {
		// initially create peers within given boundaries
		int numPeers = (configuration.getNumPeersMax() + configuration.getNumPeersMin()) / 2;
		for (int i = 0; i < numPeers; i++) {
			// create a new peer id
			Number160 peerId = new Number160(random);
			// disable peer verification (faster mutual acceptance)
			PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(peerId);
			peerMapConfiguration.peerVerification(false);
			PeerMap peerMap = new PeerMap(peerMapConfiguration);

			if (i == 0) {
				// create master peer
				masterPeer = new PeerBuilderDHT(new PeerBuilder(peerId).ports(configuration.getPort())
						.peerMap(peerMap).start()).storage(
						new StorageMemory(configuration.getTTLCheckIntervalInMilliseconds())).start();

				// enable replication if required
				enableReplication(masterPeer);

				logger.trace("Master Peer added to network. peer id = '{}'", masterPeer.peerID());
			} else {
				// create peer
				PeerDHT peer = new PeerBuilderDHT(new PeerBuilder(peerId).peerMap(peerMap)
						.masterPeer(masterPeer.peer()).start()).storage(
						new StorageMemory(configuration.getTTLCheckIntervalInMilliseconds())).start();

				// enable replication if required
				enableReplication(peer);

				// bootstrap to master peer
				FutureBootstrap fBoo = peer.peer().bootstrap().peerAddress(masterPeer.peerAddress()).start();
				fBoo.awaitUninterruptibly();

				peers.add(peer);
				logger.trace("Peer added to network. peer id = '{}'", peer.peerID());
			}
		}
		logger.debug("Network created. numPeers = '{}'", numPeers);
	}

	private void enableReplication(PeerDHT peer) {
		switch (configuration.getReplication()) {
			case "off":
				// don't enable any replication
				break;
			case "root":
				// set replication interval, start replication with 0-root approach
				new IndirectReplication(peer)
						.intervalMillis(configuration.getReplicationIntervalInMilliseconds())
						.replicationFactor(configuration.getReplicationFactor()).start();
				break;
			case "nRoot":
			default:
				// set replication interval, start replication with n-root approach
				new IndirectReplication(peer)
						.intervalMillis(configuration.getReplicationIntervalInMilliseconds()).nRoot()
						.replicationFactor(configuration.getReplicationFactor()).start();
		}
	}

	public void startChurn() {
		if (configuration.getChurnStrategyName().equals("off")) {
			logger.debug("No churn enabled.");
		} else {
			churnExecutor = new ChurnExecutor();
			churnExecutor.start();
			logger.debug("Churn started.");
		}
	}

	public void startPutting() {
		putCoordinators = new PutCoordinator[configuration.getNumKeys()];
		for (int i = 0; i < putCoordinators.length; i++) {
			putCoordinators[i] = new PutCoordinator();
		}
		logger.debug("Putting started.");
	}

	public boolean isRunning() {
		boolean shutdown = true;
		for (PutCoordinator putCoordinator : putCoordinators) {
			for (PutExecutor putExecutor : putCoordinator.putExecutors) {
				shutdown &= putExecutor.isShutdown();
			}
		}
		return !shutdown;
	}

	public void shutDownChurn() {
		if (churnExecutor != null) {
			churnExecutor.shutdown();
		}
		logger.debug("Stopped churn.");
	}

	public void shutDownPutCoordinators() {
		if (putCoordinators != null) {
			for (int i = 0; i < putCoordinators.length; i++) {
				putCoordinators[i].shutdown();
			}
		}
		logger.debug("Stopped put coordinators.");
	}

	public void shutDownNetwork() {
		for (PeerDHT peer : peers) {
			logger.trace("Shutdown peer. peer id = '{}'", peer.peerID());
			peer.shutdown().awaitUninterruptibly();
		}
		peers.clear();
		logger.debug("Shutdown of peers finished.");

		if (masterPeer != null) {
			masterPeer.shutdown().awaitUninterruptibly();
		}
		logger.debug("Shutdown of master peer finished.");
	}

	public void loadAndStoreResults() {
		int versionWrites = 0;
		int presentVersions = 0;
		for (int i = 0; i < putCoordinators.length; i++) {
			versionWrites += putCoordinators[i].countWriteExecutions();
			presentVersions += putCoordinators[i].loadLatestVersion().length();
		}
		// store settings and results in a file
		Outcome.writeResult(configuration, presentVersions, versionWrites);
	}

	public void printResults() {
		for (int i = 0; i < putCoordinators.length; i++) {
			putCoordinators[i].printResults();
		}
	}

	/**
	 * Gets repeatedly executed. Adds and removes peers according given churn
	 * strategy and join/leave ratio.
	 * 
	 * @author Seppi
	 */
	private final class ChurnExecutor extends Executor {

		private final Logger logger = LoggerFactory.getLogger(ChurnExecutor.class);

		private final double churnJoinLeaveRate;
		private final ChurnStrategy churnStrategy;

		public ChurnExecutor() {
			super(Executors.newScheduledThreadPool(1), configuration.getChurnRateMinDelayInMilliseconds(),
					configuration.getChurnRateMaxDelayInMilliseconds(), -1);

			this.churnJoinLeaveRate = configuration.getChurnJoinLeaveRate();
			logger.trace("churn join/leave rate = '{}'", churnJoinLeaveRate);

			String churnStrategyName = configuration.getChurnStrategyName();
			logger.trace("churn strategy name = '{}'", churnStrategyName);
			switch (churnStrategyName) {
				case StepwiseChurnStrategy.CHURN_STRATEGY_NAME:
					churnStrategy = new StepwiseChurnStrategy(configuration);
					break;
				case StepwiseRandomChurnStrategy.CHURN_STRATEGY_NAME:
					churnStrategy = new StepwiseRandomChurnStrategy(configuration);
					break;
				case WildChurnStrategy.CHURN_STRATEGY_NAME:
					churnStrategy = new WildChurnStrategy(configuration);
					break;
				default:
					churnStrategy = new StepwiseChurnStrategy(configuration);
					logger.warn(
							"An unknown chrun strategy name '{}' was given. Selected '{}' as default churn strategy.",
							churnStrategyName, StepwiseChurnStrategy.CHURN_STRATEGY_NAME);
			}
		}

		@Override
		public void execute() throws Exception {
			// toggle join/leaves
			double churnRate = random.nextDouble();
			if (churnJoinLeaveRate < churnRate) {
				addPeersToTheNetwork();
			} else {
				removePeersFromNetwork();
			}
		}

		private void addPeersToTheNetwork() {
			int numberOfPeerToJoin = churnStrategy.getNumJoiningPeers(peers.size());
			logger.debug("Joining {} peers. # peer = '{}'", numberOfPeerToJoin, peers.size() + 1);
			for (int i = 0; i < numberOfPeerToJoin; i++) {
				try {
					// create new peer to join
					Number160 peerId = new Number160(random);
					PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(peerId);
					peerMapConfiguration.peerVerification(false);
					PeerMap peerMap = new PeerMap(peerMapConfiguration);
					PeerDHT newPeer = new PeerBuilderDHT(new PeerBuilder(peerId).peerMap(peerMap)
							.masterPeer(masterPeer.peer()).start()).storage(
							new StorageMemory(configuration.getTTLCheckIntervalInMilliseconds())).start();

					// enable replication if required
					enableReplication(newPeer);

					// bootstrap to master peer
					FutureBootstrap futureBootstrap = newPeer.peer().bootstrap()
							.peerAddress(masterPeer.peerAddress()).start();
					futureBootstrap.awaitUninterruptibly();

					peers.add(newPeer);
					logger.trace("New peer joined the network. peer id = '{}'", newPeer.peerID());
				} catch (IOException e) {
					logger.error("Couldn't create a new peer.", e);
				}
			}
		}

		private void removePeersFromNetwork() {
			int numberOfLeavingPeers = churnStrategy.getNumLeavingPeers(peers.size());
			logger.debug("Leaving {} peers. # peers = '{}'", numberOfLeavingPeers, peers.size() + 1);
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
				logger.trace("Peer leaved the network. peer id = '{}'", peer.peerID());
			}
		}
	}

	private final class PutCoordinator {

		private final PutExecutor[] putExecutors;
		private final Number480 key;

		private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(configuration
				.getPutConcurrencyFactor());

		private String latestVersion;

		public PutCoordinator() {
			this.key = new Number480(random);
			this.putExecutors = new PutExecutor[configuration.getPutConcurrencyFactor()];
			for (int i = 0; i < putExecutors.length; i++) {
				String id = String.valueOf((char) ('a' + i));
				PutExecutor putExecutor = new PutExecutor(id, key, scheduler);
				putExecutor.start();
				putExecutors[i] = putExecutor;
			}
		}

		public String loadLatestVersion() {
			KeyLock<Number160>.RefCounterLock lock = null;
			PeerDHT peer = null;
			while (lock == null) {
				peer = peers.get(random.nextInt(peers.size()));
				lock = keyLock.tryLock(peer.peerID());
			}
			try {
				// load latest string
				FutureGet futureGet = peer.get(key.locationKey()).contentKey(key.contentKey())
						.domainKey(key.domainKey()).getLatest().start();
				futureGet.awaitUninterruptibly();

				latestVersion = (String) futureGet.data().object();
			} catch (Exception e) {
				logger.error("Couldn't get result.", e);
			} finally {
				keyLock.unlock(lock);
			}
			return latestVersion;
		}

		public int countWriteExecutions() {
			int count = 0;
			for (int i = 0; i < putExecutors.length; i++) {
				count += putExecutors[i].putStrategy.getPutCounter();
			}
			return count;
		}

		public void printResults() {
			logger.debug("latest version = '{}'", latestVersion);
			for (int i = 0; i < putExecutors.length; i++) {
				int count = 0;
				for (int j = 0; j < latestVersion.length(); j++) {
					if (latestVersion.charAt(j) == putExecutors[i].getId().charAt(0)) {
						count++;
					}
				}
				// print results
				logger.debug("version writes = '{}' present versions = '{}' id = '{}' key = '{}'",
						putExecutors[i].putStrategy.getPutCounter(), count, putExecutors[i].id, key);
				putExecutors[i].putStrategy.printResults();
			}
		}

		public void shutdown() {
			for (int i = 0; i < putExecutors.length; i++) {
				putExecutors[i].shutdown();
			}
		}

	}

	private final class PutExecutor extends Executor {

		private final Logger logger = LoggerFactory.getLogger(PutExecutor.class);

		private final Number480 key;
		private final PutStrategy putStrategy;
		private final String id;

		public PutExecutor(String id, Number480 key, ScheduledExecutorService scheduler) {
			super(scheduler, configuration.getPutDelayMinInMilliseconds(), configuration
					.getPutDelayMaxInMilliseconds(), configuration.getNumPuts());
			this.key = key;
			this.id = id;

			String putApproach = configuration.getPutStrategyName();
			switch (putApproach) {
				case TraditionalPutStrategy.PUT_STRATEGY_NAME:
					putStrategy = new TraditionalPutStrategy(id, key);
					break;
				case TraditionalVersionPutStrategy.PUT_STRATEGY_NAME:
					putStrategy = new TraditionalVersionPutStrategy(id, key, configuration);
					break;
				case OptimisticPutStrategy.PUT_STRATEGY_NAME:
					putStrategy = new OptimisticPutStrategy(id, key, configuration);
					break;
				case PesimisticPutStrategy.PUT_STRATEGY_NAME:
					putStrategy = new PesimisticPutStrategy(id, key, configuration);
					break;
				default:
					putStrategy = new OptimisticPutStrategy(id, key, configuration);
					logger.warn(
							"An unknown put approach '{}' was given. Selected '{}' as default put approach.",
							putApproach, OptimisticPutStrategy.PUT_STRATEGY_NAME);
			}
		}

		@Override
		public void execute() throws Exception {
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
			logger.trace("Put. key = '{}'", key);
		}

		public String getId() {
			return id;
		}
	}

}
