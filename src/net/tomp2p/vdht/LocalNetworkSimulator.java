package net.tomp2p.vdht;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.StorageLayer.PutStatus;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.replication.IndirectReplication;
import net.tomp2p.storage.Data;
import net.tomp2p.vdht.churn.ChurnStrategy;
import net.tomp2p.vdht.churn.StepwiseChurnStrategy;
import net.tomp2p.vdht.churn.StepwiseRandomChurnStrategy;
import net.tomp2p.vdht.churn.WildChurnStrategy;
import net.tomp2p.vdht.put.OptimisticPutStrategy;
import net.tomp2p.vdht.put.PesimisticPutStrategy;
import net.tomp2p.vdht.put.PutStrategy;
import net.tomp2p.vdht.put.TraditionalPutStrategy;

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

	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	private ScheduledFuture<?> churnFuture;
	private ScheduledFuture<?>[][] putFutures;

	private final Random random = new Random();
	private final KeyLock<Number160> keyLock = new KeyLock<Number160>();
	private final Set<Number480> keySet = new HashSet<Number480>();

	private final int numPeersMax;
	private final int numPeersMin;
	private final int port;
	private final int numKeys;
	private final int putConcurrencyFactor;

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
				masterPeer = new PeerDHT(new PeerBuilder(peerId).ports(port).peerMap(peerMap)
						.allPeersReplicate(true).start());

				// enable replication if required
				enableReplication(masterPeer);

				logger.trace("Master Peer added to network. peer id = '{}'", masterPeer.peerID());
			} else {
				// create peer
				PeerDHT peer = new PeerDHT(new PeerBuilder(peerId).peerMap(peerMap)
						.masterPeer(masterPeer.peer()).start());

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
			new IndirectReplication(peer).start();
			break;
		case "nRoot":
		default:
			new IndirectReplication(peer).nRoot().start();
		}
	}

	public void startChurn() throws IOException {
		if (Configuration.getChurnStrategyName().equals("off")) {
			logger.debug("No churn enabled.");
		} else {
			ChurnExecutor churnExecutor = new ChurnExecutor(scheduler);
			churnFuture = scheduler.schedule(churnExecutor, churnExecutor.delay(), TimeUnit.MILLISECONDS);
			logger.debug("Churn started.");
		}
	}

	public void startPutting() throws IOException {
		putFutures = new ScheduledFuture<?>[numKeys][putConcurrencyFactor];
		for (int i = 0; i < numKeys; i++) {
			Number480 key = new Number480(random);
			PutExecutor putExecutor = new PutExecutor(key, scheduler);
			// start putting with same key according concurrency factor
			for (int j = 0; j < putConcurrencyFactor; j++) {
				putFutures[i][j] = scheduler
						.schedule(putExecutor, putExecutor.delay(), TimeUnit.MILLISECONDS);
			}
			// store key for future analysis
			keySet.add(key);
		}
		logger.debug("Putting started.");
	}

	// TODO
//	public void verifyPutResults() throws ClassNotFoundException, IOException {
//		for (Number480 key : keySet) {
//			FutureGet futureGet = masterPeer.get(key.locationKey()).domainKey(key.domainKey())
//					.contentKey(key.contentKey()).getLatest().start();
//			futureGet.awaitUninterruptibly();
//
//			Map<Number640, Data> dataMap = futureGet.dataMap();
//			for (Number640 k : dataMap.keySet()) {
//				String value = (String) dataMap.get(k).object();
//				char[] valueChar = value.toCharArray();
//				for (int i = 0; i < valueChar.length; i++) {
//					char c = (char) ('a' + (i % 26));
//					if (valueChar[i] != c) {
//						logger.debug("Detected some inconsistency. key = '{}'", key);
//						break;
//					}
//				}
//			}
//		}
//	}

	public void shutDownNetwork() {
		if (churnFuture != null) {
			churnFuture.cancel(true);
		}
		if (putFutures != null) {
			for (int i = 0; i < putFutures.length; i++) {
				for (int j = 0; j < putFutures[i].length; j++) {
					putFutures[i][j].cancel(true);
				}
			}
		}

		scheduler.shutdown();
		try {
			scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			logger.error("Couldn't wait for termination of scheduled tasks.", e);
		}

		for (PeerDHT peer : peers) {
			logger.trace("Shutdown peer. peer id = '{}'", peer.peerID());
			peer.shutdown().awaitUninterruptibly();
		}
		peers.clear();
		if (masterPeer != null) {
			masterPeer.shutdown().awaitUninterruptibly();
		}
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

		public ChurnExecutor(ScheduledExecutorService scheduler) throws IOException {
			super(scheduler, Configuration.getChurnRateMinDelayInMilliseconds(), Configuration
					.getChurnRateMaxDelayInMilliseconds(), -1);

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
			logger.debug("Currently online peers = '{}'", peers.size() + 1);
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
			logger.debug("Joining {} peers.", numberOfPeerToJoin);
			for (int i = 0; i < numberOfPeerToJoin; i++) {
				try {
					// create new peer to join
					PeerDHT newPeer = new PeerDHT(new PeerBuilder(new Number160(random)).masterPeer(
							masterPeer.peer()).start());
					new IndirectReplication(masterPeer).nRoot().start();

					// enable replication if required
					enableReplication(newPeer);

					// bootstrap to master peer
					FutureBootstrap fBoo = newPeer.peer().bootstrap().peerAddress(masterPeer.peerAddress())
							.start();
					fBoo.awaitUninterruptibly();

					peers.add(newPeer);
					logger.trace("New peer joined the network. peer id = '{}'", newPeer.peerID());
				} catch (IOException e) {
					logger.error("Couldn't create a new peer.", e);
				}
			}
		}

		private void removePeersFromNetwork() {
			int numberOfLeavingPeers = churnStrategy.getNumLeavingPeers(peers.size());
			logger.debug("Leaving {} peers.", numberOfLeavingPeers);
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

	private final class PutExecutor extends Executor {

		private final Logger logger = LoggerFactory.getLogger(PutExecutor.class);

		private final Number480 key;
		private final PutStrategy putStrategy;

		public PutExecutor(Number480 key, ScheduledExecutorService scheduler) throws IOException {
			super(scheduler, Configuration.getPutDelayMinInMilliseconds(), Configuration
					.getPutDelayMaxInMilliseconds(), Configuration.getNumPuts());
			this.key = key;

			String putApproach = Configuration.getPutApproach();
			switch (putApproach) {
			case TraditionalPutStrategy.PUT_STRATEGY_NAME:
				putStrategy = new TraditionalPutStrategy(key);
				break;
			case OptimisticPutStrategy.PUT_STRATEGY_NAME:
				putStrategy = new OptimisticPutStrategy();
				break;
			case PesimisticPutStrategy.PUT_STRATEGY_NAME:
				putStrategy = new PesimisticPutStrategy();
				break;
			default:
				putStrategy = new OptimisticPutStrategy();
				logger.warn("An unknown put approach '{}' was given. Selected '{}' as default put approach.",
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
