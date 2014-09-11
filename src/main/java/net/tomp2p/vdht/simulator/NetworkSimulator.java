package net.tomp2p.vdht.simulator;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.DefaultMaintenance;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.replication.IndirectReplication;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.KeyLock;
import net.tomp2p.vdht.churn.ChurnExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A network simulator setting up a local peer-to-peer network. Churn can be
 * enabled. All configuration is fetched from {@link Configuration} class.
 * 
 * @author Seppi
 */
public class NetworkSimulator implements ISimulator {

	private static Logger logger = LoggerFactory
			.getLogger(NetworkSimulator.class);

	private PeerDHT masterPeer;
	private final List<PeerDHT> peers = Collections
			.synchronizedList(new ArrayList<PeerDHT>());
	private final List<PeerDHT> lockedPeers = Collections
			.synchronizedList(new ArrayList<PeerDHT>());

	private ChurnExecutor churnExecutor;

	private final Random random = new Random();
	private final KeyLock<Number160> keyLock = new KeyLock<Number160>();
	private final Map<PeerDHT, KeyLock<Number160>.RefCounterLock> locks = Collections
			.synchronizedMap(new HashMap<PeerDHT, KeyLock<Number160>.RefCounterLock>());

	private final Configuration configuration;

	public NetworkSimulator(Configuration configuration) {
		this.configuration = configuration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public int getNetworkSize() {
		int tmp = peers.size() + lockedPeers.size();
		if (masterPeer != null) {
			tmp += 1;
		}
		return tmp;
	}

	@Override
	public void createNetwork() throws IOException {
		createNetwork("local", -1);
	}

	@Override
	public void createNetwork(String bootstrapIP, int bootstrapPort)
			throws IOException {
		// initially create peers within given boundaries
		int numPeers = (configuration.getNumPeersMax() + configuration
				.getNumPeersMin()) / 2;
		for (int i = 0; i < numPeers; i++) {
			if (i == 0) {
				// create master peer
				masterPeer = createPeer(true);

				// enable replication if required
				enableReplication(masterPeer);

				if (!bootstrapIP.equals("local") && bootstrapPort > 0) {
					FutureDiscover futureDiscover = masterPeer.peer()
							.discover()
							.inetAddress(Inet4Address.getByName(bootstrapIP))
							.ports(bootstrapPort).start();
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

					FutureBootstrap futureBootstrap = masterPeer.peer()
							.bootstrap()
							.inetAddress(Inet4Address.getByName(bootstrapIP))
							.ports(bootstrapPort).start();
					futureBootstrap.awaitUninterruptibly();

					if (futureBootstrap.isSuccess()) {
						logger.debug(
								"Bootstrapping successful. Bootstrapped to '{}'.",
								bootstrapIP);
					} else {
						logger.warn("Bootstrapping failed: {}.",
								futureBootstrap.failedReason());
						throw new IllegalStateException("Bootstraping failed.");
					}
				}
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
		// set higher peer map update frequency
		peerMapConfiguration.maintenance(new DefaultMaintenance(4,
				new int[] { 1 }));
		// only one try required to label a peer as offline
		peerMapConfiguration.offlineCount(1);
		peerMapConfiguration.shutdownTimeout(1);
		PeerMap peerMap = new PeerMap(peerMapConfiguration);
		// reduce TCP number of short-lived TCP connections to avoid timeouts
		ChannelClientConfiguration channelConfig = PeerBuilder
				.createDefaultChannelClientConfiguration();
		channelConfig.maxPermitsTCP(20);

		final PeerDHT peer = new PeerBuilderDHT(new PeerBuilder(peerId)
				.ports(configuration.getPort()).peerMap(peerMap)
				.masterPeer(isMaster ? null : masterPeer.peer())
				.channelClientConfiguration(channelConfig)
				// .maintenanceTask(new MaintenanceTask().intervalMillis(100))
				.start()).storage(
				new StorageMemory(configuration
						.getTTLCheckIntervalInMilliseconds(), configuration
						.getMaxVersions())).start();

		return peer;
	}

	private void enableReplication(PeerDHT peer) {
		switch (configuration.getReplicationStrategyName()) {
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
					.keepData(true).start();
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
					.keepData(true).start();
		}
	}

	@Override
	public void startChurn() {
		if (configuration.getChurnStrategyName().equals("off")) {
			logger.debug("No churn enabled.");
		} else {
			churnExecutor = new ChurnExecutor(this);
			churnExecutor.start();
			logger.debug("Churn started.");
		}
	}

	@Override
	public boolean isChurnRunning() {
		return !churnExecutor.isShutdown();
	}

	@Override
	public void shutDownChurn() throws InterruptedException {
		if (churnExecutor != null) {
			churnExecutor.shutdown();
			while (isChurnRunning()) {
				Thread.sleep(100);
			}
		}
	}

	@Override
	public void shutDownNetwork() {
		for (PeerDHT peer : peers) {
			peer.shutdown().awaitUninterruptibly();
		}
		peers.clear();
		for (PeerDHT peer: lockedPeers) {
			peer.shutdown().awaitUninterruptibly();
		}
		lockedPeers.clear();
		if (masterPeer != null) {
			masterPeer.shutdown().awaitUninterruptibly();
		}
		masterPeer = null;
	}

	@Override
	public PeerDHT requestPeer(boolean permanent) {
		KeyLock<Number160>.RefCounterLock lock = null;
		PeerDHT peer = null;
		while (lock == null) {
			peer = peers.get(random.nextInt(peers.size()));
			lock = keyLock.tryLock(peer.peerID());
		}
		locks.put(peer, lock);
		peers.remove(peer);
		lockedPeers.add(peer);
		return peer;
	}

	@Override
	public void releasePeer(PeerDHT peer) throws IllegalMonitorStateException {
		keyLock.unlock(locks.remove(peer));
		if (lockedPeers.remove(peer)) {
			peers.add(peer);
		}
	}

	@Override
	public void addPeersToTheNetwork(int numberOfPeerToJoin) {
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
				logger.debug("Peer '{}' joined the network.", newPeer.peerID());
			} catch (IOException e) {
				logger.error("Couldn't create a new peer.", e);
			}
		}
		logger.debug("{} peers online.", getNetworkSize());
	}

	@Override
	public void removePeersFromNetwork(int numberOfLeavingPeers) {
		for (int i = 0; i < numberOfLeavingPeers; i++) {
			PeerDHT peer = requestPeer(false);
			peers.remove(peer);
			peer.peer().announceShutdown().start().awaitUninterruptibly();
			peer.shutdown().awaitUninterruptibly();
			releasePeer(peer);
			logger.debug("Peer '{}' leaved the network.", peer.peerID());
		}
		logger.debug("{} peers online.", getNetworkSize());
	}

	@Override
	public void removeClosePeersFromNetwork(Number160 locationKey,
			int numLeavingPeers) {
		NavigableMap<Number160, PeerDHT> map = new TreeMap<Number160, PeerDHT>(
				PeerMap.createComparator2(locationKey));
		for (PeerDHT peer : peers) {
			map.put(peer.peerID(), peer);
		}
		int i = 0;
		for (Iterator<Number160> iterator = map.keySet().iterator(); iterator
				.hasNext() && numLeavingPeers > i; i++) {
			Number160 toRemoveKey = iterator.next();
			PeerDHT toRemovePeer = map.get(toRemoveKey);

			KeyLock<Number160>.RefCounterLock lock = null;
			while (lock == null) {
				lock = keyLock.tryLock(toRemoveKey);
			}
			peers.remove(toRemovePeer);
			toRemovePeer.peer().announceShutdown().start()
					.awaitUninterruptibly();
			toRemovePeer.shutdown().awaitUninterruptibly();
			keyLock.unlock(lock);
			logger.debug("Peer '{}' close to '{}' leaved the network.",
					toRemovePeer.peerID(), locationKey);
		}
		logger.debug("{} peers online.", getNetworkSize());
	}

	@Override
	public PeerDHT getMasterPeer() {
		return masterPeer;
	}

}
