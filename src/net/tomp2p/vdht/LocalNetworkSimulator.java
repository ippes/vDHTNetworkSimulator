package net.tomp2p.vdht;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.StorageLayer;
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

	private final int numPeersMax;
	private final int numPeersMin;
	private final int port;
	private final int numKeys;
	private final int putConcurrencyFactor;
	private final int ttlCheckIntervalInMilliseconds;
	private final int replicationFactor;
	private final int replicationIntervalInMilliseconds;

	public LocalNetworkSimulator() throws IOException {
		this.numPeersMax = Configuration.getNumPeersMax();
		logger.trace("max # peers = '{}", numPeersMax);
		this.numPeersMin = Configuration.getNumPeersMin();
		logger.trace("min # peers = '{}", numPeersMin);
		this.port = Configuration.getPort();
		logger.trace("port = '{}'", port);
		this.numKeys = Configuration.getNumKeys();
		logger.trace("# keys = '{}'", numKeys);
		this.putConcurrencyFactor = Configuration.getPutConcurrencyFactor();
		logger.trace("put concurrency factor = '{}'", putConcurrencyFactor);
		this.ttlCheckIntervalInMilliseconds = Configuration.getTTLCheckIntervalInMilliseconds();
		logger.trace("ttl check interval in milliseconds = '{}'", ttlCheckIntervalInMilliseconds);
		this.replicationFactor = Configuration.getReplicationFactor();
		logger.trace("replication factor = '{}'", replicationFactor);
		this.replicationIntervalInMilliseconds = Configuration.getReplicationIntervalInMilliseconds();
		logger.trace("replication interval in milliseconds = '{}'", replicationIntervalInMilliseconds);
	}

	public void createNetwork() throws Exception {
		// initially create peers within given boundaries
		int numPeers = (numPeersMax + numPeersMin) / 2;
		for (int i = 0; i < numPeers; i++) {
			// create a new peer id
			Number160 peerId = new Number160(random);
			// disable peer verification (faster mutual acceptance)
			PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(peerId);
			peerMapConfiguration.peerVerification(false);
			PeerMap peerMap = new PeerMap(peerMapConfiguration);

			if (i == 0) {
				// create master peer
				masterPeer = new PeerDHT(new PeerBuilder(peerId).ports(port).peerMap(peerMap).start(),
						new StorageLayer(new StorageMemory(ttlCheckIntervalInMilliseconds)));

				// enable replication if required
				enableReplication(masterPeer);

				logger.trace("Master Peer added to network. peer id = '{}'", masterPeer.peerID());
			} else {
				// create peer
				PeerDHT peer = new PeerDHT(new PeerBuilder(peerId).peerMap(peerMap)
						.masterPeer(masterPeer.peer()).start(), new StorageLayer(new StorageMemory(
						ttlCheckIntervalInMilliseconds)));

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

	private void enableReplication(PeerDHT peer) throws IOException {
		switch (Configuration.getReplication()) {
			case "off":
				// don't enable any replication
				break;
			case "root":
				// set replication interval, start replication with 0-root approach
				new IndirectReplication(peer).intervalMillis(replicationIntervalInMilliseconds)
						.replicationFactor(replicationFactor).start();
				break;
			case "nRoot":
			default:
				// set replication interval, start replication with n-root approach
				new IndirectReplication(peer).intervalMillis(replicationIntervalInMilliseconds).nRoot()
						.replicationFactor(replicationFactor).start();
		}
	}

	public void startChurn() throws IOException {
		if (Configuration.getChurnStrategyName().equals("off")) {
			logger.debug("No churn enabled.");
		} else {
			churnExecutor = new ChurnExecutor();
			churnExecutor.start();
			logger.debug("Churn started.");
		}
	}

	public void startPutting() throws IOException {
		putCoordinators = new PutCoordinator[numKeys];
		for (int i = 0; i < numKeys; i++) {
			putCoordinators[i] = new PutCoordinator();
		}
		logger.debug("Putting started.");
	}

	public void shutDown() {
		if (putCoordinators != null) {
			for (int i = 0; i < putCoordinators.length; i++) {
				putCoordinators[i].shutdown();
			}
		}
		logger.debug("Stopped put coordinators.");

		if (churnExecutor != null) {
			churnExecutor.shutdown();
		}
		logger.debug("Stopped churn.");

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
		logger.debug("Shutdown of network finished.");
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

		public ChurnExecutor() throws IOException {
			super(Executors.newScheduledThreadPool(1), Configuration.getChurnRateMinDelayInMilliseconds(),
					Configuration.getChurnRateMaxDelayInMilliseconds(), -1);

			this.churnJoinLeaveRate = Configuration.getChurnJoinLeaveRate();
			logger.trace("churn join/leave rate = '{}'", churnJoinLeaveRate);

			String churnStrategyName = Configuration.getChurnStrategyName();
			logger.trace("churn strategy name = '{}'", churnStrategyName);
			switch (churnStrategyName) {
				case StepwiseChurnStrategy.CHURN_STRATEGY_NAME:
					churnStrategy = new StepwiseChurnStrategy();
					break;
				case StepwiseRandomChurnStrategy.CHURN_STRATEGY_NAME:
					churnStrategy = new StepwiseRandomChurnStrategy();
					break;
				case WildChurnStrategy.CHURN_STRATEGY_NAME:
					churnStrategy = new WildChurnStrategy();
					break;
				default:
					churnStrategy = new StepwiseChurnStrategy();
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
					PeerDHT newPeer = new PeerDHT(new PeerBuilder(new Number160(random)).masterPeer(
							masterPeer.peer()).start(), new StorageLayer(new StorageMemory(
							ttlCheckIntervalInMilliseconds)));

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

		private final ScheduledExecutorService scheduler = Executors
				.newScheduledThreadPool(putConcurrencyFactor);

		public PutCoordinator() throws IOException {
			this.key = new Number480(random);
			this.putExecutors = new PutExecutor[putConcurrencyFactor];
			for (int i = 0; i < putConcurrencyFactor; i++) {
				String id = String.valueOf((char) ('a' + i));
				PutExecutor putExecutor = new PutExecutor(id, key, scheduler);
				putExecutor.start();
				putExecutors[i] = putExecutor;
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

		public PutExecutor(String id, Number480 key, ScheduledExecutorService scheduler) throws IOException {
			super(scheduler, Configuration.getPutDelayMinInMilliseconds(), Configuration
					.getPutDelayMaxInMilliseconds(), Configuration.getNumPuts());
			this.key = key;

			String putApproach = Configuration.getPutStrategyName();
			switch (putApproach) {
				case TraditionalPutStrategy.PUT_STRATEGY_NAME:
					putStrategy = new TraditionalPutStrategy(id, key);
					break;
				case TraditionalVersionPutStrategy.PUT_STRATEGY_NAME:
					putStrategy = new TraditionalVersionPutStrategy(id, key);
					break;
				case OptimisticPutStrategy.PUT_STRATEGY_NAME:
					putStrategy = new OptimisticPutStrategy(id, key);
					break;
				case PesimisticPutStrategy.PUT_STRATEGY_NAME:
					putStrategy = new PesimisticPutStrategy(id, key);
					break;
				default:
					putStrategy = new OptimisticPutStrategy(id, key);
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

	}

}
