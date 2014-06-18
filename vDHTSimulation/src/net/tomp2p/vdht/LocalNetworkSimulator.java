package net.tomp2p.vdht;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.vdht.churn.ChurnStrategy;
import net.tomp2p.vdht.churn.StepwiseChurnStrategy;
import net.tomp2p.vdht.churn.StepwiseRandomChurnStrategy;
import net.tomp2p.vdht.churn.WildChurnStrategy;

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

	private final KeyLock<Number160> keyLock = new KeyLock<Number160>();

	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	private ScheduledFuture<?> churnFuture;
	private ScheduledFuture<?>[] putFutures;

	private final Random random = new Random();

	private final int numPeersMax;
	private final int port;
	private final int churnRateDelayInMilliseconds;
	private final int numKeys;

	public LocalNetworkSimulator() throws IOException {
		this.numPeersMax = Configuration.getNumPeersMax();
		logger.trace("max # peers = '{}", numPeersMax);
		this.port = Configuration.getPort();
		logger.trace("port = '{}'", port);
		this.churnRateDelayInMilliseconds = Configuration.getChurnRateDelayInMilliseconds();
		logger.trace("churn rate delay in milliseconds = '{}'", churnRateDelayInMilliseconds);
		this.numKeys = Configuration.getNumKeys();
		logger.trace("# keys = '{}'", numKeys);
	}

	public void createNetwork() throws Exception {
		for (int i = 0; i < numPeersMax; i++) {
			if (i == 0) {
				masterPeer = new PeerDHT(new PeerBuilder(new Number160(random)).ports(port).start());
				logger.trace("Master Peer added to network. peer id = '{}'", masterPeer.peerID());
			} else {
				PeerDHT peer = new PeerDHT(new PeerBuilder(new Number160(random)).masterPeer(
						masterPeer.peer()).start());
				peers.add(peer);
				logger.trace("Peer added to network. peer id = '{}'", peer.peerID());
			}
		}
		logger.debug("Network created.");
	}

	public void startChurn() throws IOException {
		churnFuture = scheduler.scheduleWithFixedDelay(new ChurnExecutor(), 0, churnRateDelayInMilliseconds,
				TimeUnit.MILLISECONDS);
		logger.debug("Churn started.");
	}

	public void startPutting() throws IOException {
		putFutures = new ScheduledFuture<?>[numKeys];
		for (int i = 0; i < numKeys; i++) {
			PutExecutor putExecutor = new PutExecutor(scheduler);
			putFutures[i] = scheduler.schedule(putExecutor, putExecutor.delay(), TimeUnit.MILLISECONDS);
		}
		logger.debug("Putting started.");
	}

	public void shutDownNetwork() {
		if (churnFuture != null) {
			churnFuture.cancel(true);
		}
		if (putFutures != null) {
			for (int i = 0; i < putFutures.length; i++) {
				putFutures[i].cancel(true);
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
	private final class ChurnExecutor implements Runnable {

		private final Logger logger = LoggerFactory.getLogger(ChurnExecutor.class);

		private final double churnJoinLeaveRate;
		private final ChurnStrategy churnStrategy;

		public ChurnExecutor() throws IOException {
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
						"An unknown strategy name '{}' was given. Selected '{}' as default churn strategy.",
						churnStrategyName, StepwiseChurnStrategy.CHURN_STRATEGY_NAME);
			}
		}

		@Override
		public void run() {
			try {
				logger.debug("Currently online peers = '{}'", peers.size() + 1);
				// toggle join/leaves
				double churnRate = random.nextDouble();
				if (churnJoinLeaveRate < churnRate) {
					addPeersToTheNetwork();
				} else {
					removePeersFromNetwork();
				}
			} catch (Exception e) {
				logger.error("Caught an unexpected exception.", e);
			}
		}

		private void addPeersToTheNetwork() {
			int numberOfPeerToJoin = churnStrategy.getNumJoiningPeers(peers.size());
			logger.debug("Joining {} peers.", numberOfPeerToJoin);
			for (int i = 0; i < numberOfPeerToJoin; i++) {
				try {
					PeerDHT newPeer = new PeerDHT(new PeerBuilder(new Number160(random)).masterPeer(
							masterPeer.peer()).start());
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
				int randomIndex = random.nextInt(peers.size());
				PeerDHT peer = peers.get(randomIndex);
				peers.remove(peer);
				peer.shutdown().awaitUninterruptibly();
				logger.trace("Peer leaved the network. peer id = '{}'", peer.peerID());
			}
		}
	}

	private final class PutExecutor implements Runnable {

		private final Logger logger = LoggerFactory.getLogger(PutExecutor.class);

		private final ScheduledExecutorService scheduler;
		private final Number480 key;

		private final int putDelayMaxInMilliseconds;
		private final int putDelayMinInMilliseconds;

		public PutExecutor(ScheduledExecutorService scheduler) throws IOException {
			this.scheduler = scheduler;
			this.key = new Number480(random);
			this.putDelayMaxInMilliseconds = Configuration.getPutDelayMaxInMilliseconds();
			logger.trace("max put delay in milliseconds = '{}'", putDelayMaxInMilliseconds);
			this.putDelayMinInMilliseconds = Configuration.getPutDelayMinInMilliseconds();
			logger.trace("min put delay in milliseconds = '{}'", putDelayMinInMilliseconds);
		}

		@Override
		public void run() {
			try {
				// // select random online peer
				// PeerDHT peer = null;
				// int randomPeerIndex = random.nextInt(peers.size() + 1) - 1;
				// peer = peers.get(randomPeerIndex);
				//
				// Data data = null;
				// try {
				// data = new Data("test");
				// } catch (IOException e) {
				// logger.error("Couldn't create data object.", e);
				// return;
				// }
				//
				// FuturePut futurePut =
				// peer.put(key.locationKey()).domainKey(key.domainKey())
				// .data(key.contentKey(), data).start();
				// futurePut.awaitUninterruptibly();
				//
				// logger.debug("Put lKey = '{}' success = '{}'",
				// key.locationKey(),
				// futurePut.isSuccess());
			} catch (Exception e) {
				logger.error("Caught an unexpected exception.", e);
			} finally {
				// schedule next put with a varying delay
				int delay = delay();
				logger.debug("Scheduling a new put task in '{}' milliseconds.", delay);
				scheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
			}
		}

		/**
		 * Calculates a varying delay within given boundaries.
		 * 
		 * @return a delay
		 */
		public int delay() {
			int maxDelta = putDelayMaxInMilliseconds - putDelayMinInMilliseconds;
			int varyingDelta = maxDelta > 0 ? random.nextInt(maxDelta + 1) : 0;
			return putDelayMinInMilliseconds + varyingDelta;
		}

	}

}
