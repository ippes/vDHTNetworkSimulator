package net.tomp2p.vdht;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FutureRemove;
import net.tomp2p.dht.FutureSend;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.MaintenanceTask;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.DefaultMaintenance;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.replication.IndirectReplication;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.vdht.churn.ChurnExecutor;
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

	public PutCoordinator getPutCoordinator() {
		return putCoordinator;
	}

	public int getPeerSize() {
		if (peers != null) {
			return peers.size();
		} else {
			return 0;
		}
	}

	public void createNetwork() throws IOException {
		createNetwork("local", -1);
	}

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
		channelConfig.maxPermitsTCP(configuration.getReplicationFactor() * 3);

		final PeerDHT peer = new PeerBuilderDHT(new PeerBuilder(peerId)
				.ports(configuration.getPort()).peerMap(peerMap)
				.masterPeer(isMaster ? null : masterPeer.peer())
				.channelClientConfiguration(channelConfig)
				.maintenanceTask(new MaintenanceTask().intervalMillis(100))
				.start()).storage(
				new StorageMemory(configuration
						.getTTLCheckIntervalInMilliseconds(), configuration
						.getMaxVersions())).start();

		if (!isMaster) {
			// add a message handler to handle shutdown messages
			peer.peer().objectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request)
						throws Exception {
					String command = (String) request;
					if (command.equals("shutdown")) {
						//logger.debug("I {} have to shutdown.", peer.peerID());
						new Thread(new Runnable() {
							@Override
							public void run() {
								Thread.currentThread().setName(
										"vDHT - Shutdown Peer");
								KeyLock<Number160>.RefCounterLock lock = null;
								while (lock == null) {
									lock = keyLock.tryLock(peer.peerID());
								}
								// receiving peer has to shutdown
								peers.remove(peer);
								peer.shutdown().awaitUninterruptibly();
								keyLock.unlock(lock);
							}
						}).start();
					} else if (command.equals("create")) {
						//logger.debug("I {} have to create.", peer.peerID());
						new Thread(new Runnable() {
							@Override
							public void run() {
								Thread.currentThread().setName(
										"vDHT - Create Peer");
								// create one peer
								addPeersToTheNetwork(1);
							}
						}).start();
					} else {
						logger.error("Received an unknown message '{}'.",
								request);
					}
					return null;
				}
			});
		} else {
			// add a message handler to handle shutdown messages
			peer.peer().objectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request)
						throws Exception {
					logger.debug("As master I will not {} a peer.", request);
					return null;
				}
			});
		}

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
					.keepData(false).start();
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
					.keepData(false).start();
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
		if (putCoordinator != null) {
			// load latest version
			putCoordinator.loadResults();
			// store settings and results in a file
			Outcome.writeResult(configuration, putCoordinator.getResult());
		}
	}

	public void printResults() {
		if (putCoordinator != null) {
			putCoordinator.getResult().printResults();
		}
	}

	public void deleteData() {
		if (putCoordinator != null) {
			Number480 key = putCoordinator.getKey();
			FutureRemove futureRemove = masterPeer
					.remove(key.locationKey())
					.from(new Number640(key.locationKey(), key.domainKey(), key
							.contentKey(), Number160.ZERO))
					.to(new Number640(key.locationKey(), key.domainKey(), key
							.contentKey(), Number160.MAX_VALUE)).start();
			futureRemove.awaitUninterruptibly();
		}
	}

	public void addPeersToTheNetwork(int numberOfPeerToJoin) {
		// logger.debug("Joining {} peers. # peer = '{}'", numberOfPeerToJoin,
		// peers.size() + 1);
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
			} catch (IOException e) {
				logger.error("Couldn't create a new peer.", e);
			}
		}
	}

	public void removePeersFromNetwork(int numberOfLeavingPeers) {
		// logger.debug("Leaving {} peers. # peers = '{}'",
		// numberOfLeavingPeers,
		// peers.size() + 1);
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

	public void sendShutdownMessages(Number160 key, int numPeers) {
		//logger.debug("Shutdowning {} nodes. nodes = '{}'", numPeers, peers.size());
		KeyLock<Number160>.RefCounterLock lock = null;
		PeerDHT peer = null;
		while (lock == null) {
			peer = peers.get(random.nextInt(peers.size()));
			lock = keyLock.tryLock(peer.peerID());
		}
		// contact given number of peers
		FutureSend futureSend = peer
				.send(key)
				.object("shutdown")
				.requestP2PConfiguration(
						new RequestP2PConfiguration(numPeers, 0, 0)).start();
		futureSend.awaitUninterruptibly();
		keyLock.unlock(lock);
	}

	public void sendCreateMessages(Number160 key, int numPeers) {
		//logger.debug("Creating {} nodes. nodes = '{}'", numPeers, peers.size());
		KeyLock<Number160>.RefCounterLock lock = null;
		PeerDHT peer = null;
		while (lock == null) {
			peer = peers.get(random.nextInt(peers.size()));
			lock = keyLock.tryLock(peer.peerID());
		}
		// contact given number of peers
		FutureSend futureSend = peer
				.send(key)
				.object("create")
				.requestP2PConfiguration(
						new RequestP2PConfiguration(numPeers, 0, 0)).start();
		futureSend.awaitUninterruptibly();
		keyLock.unlock(lock);
	}

}
