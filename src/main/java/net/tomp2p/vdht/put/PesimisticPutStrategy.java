package net.tomp2p.vdht.put;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.FutureRemove;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.storage.Data;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Put strategy following the pessimistic vDHT approach.
 * 
 * @author Seppi
 */
public final class PesimisticPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory
			.getLogger(PesimisticPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "pessimistic";

	private final Configuration configuration;

	private Number160 memorizedVersionKey = Number160.ZERO;
	private boolean firstTime = true;
	private int waitTime = 0;
	private Random random = new Random();

	public PesimisticPutStrategy(String id, Number480 key, Result result,
			Configuration configuration) {
		super(id, key, result);
		this.configuration = configuration;
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// repeat as long as a version can be confirmed
		while (true) {
			// get and update value from network
			Update update = getAndUpdate(peer);

			// set prepare flag
			Data updatedData = update.data;
			updatedData.prepareFlag();
			// set time to live (prepare)
			if (configuration.getPutPrepareTTLInSeconds() > 0) {
				updatedData.ttlSeconds(configuration
						.getPutPrepareTTLInSeconds());
			}

			// put updated version into network
			FuturePut futurePut;
			int counter = 0;
			while (true) {
				futurePut = peer
						.put(key.locationKey())
						.data(key.contentKey(), updatedData)
						.domainKey(key.domainKey())
						.versionKey(update.vKey)
						// put has to address the whole replica set
						.requestP2PConfiguration(
								new RequestP2PConfiguration(configuration
										.getReplicationFactor(), 0, 0)).start();
				futurePut.awaitUninterruptibly();

				if (futurePut.isFailed()) {
					logger.warn("Put failed. Retrying.");
					if (counter++ < 2) {
						new IllegalStateException("Put failed after " + counter
								+ " tries.");
					}
				} else {
					break;
				}
			}

			logger.debug("Put. value = '{}' version = '{}'",
					update.data.object(), update.vKey.timestamp());

			// check for any version forks
			if (!Utils.hasVersionForkAfterPut(futurePut.rawResult())) {
				Data data = new Data();
				if (configuration.getPutTTLInSeconds() > 0) {
					data.ttlSeconds(configuration.getPutTTLInSeconds());
				}

				// confirm put
				FuturePut futurePutConfirm = peer
						.put(key.locationKey())
						.domainKey(key.domainKey())
						.data(key.contentKey(), data)
						.versionKey(update.vKey)
						.putConfirm()
						.requestP2PConfiguration(
								new RequestP2PConfiguration(configuration
										.getReplicationFactor(), 5, 0)).start();
				futurePutConfirm.awaitUninterruptibly();

				// store version key
				if (update.vKey.compareTo(memorizedVersionKey) < 0) {
					memorizedVersionKey = update.vKey;
				}

				firstTime = false;
				waitTime = 0;
				if (update.isMerge) {
					increaseMergeCounter();
				} else {
					increaseWriteCounter();
				}

				logger.debug("Put confirmed. write counter = '{}'",
						getWriteCounter());

				break;
			} else {
				logger.warn("Version fork after put detected. Rejecting and retrying put.");

				// reject put
				FutureRemove futureRemove;
				do {
					futureRemove = peer
							.remove(key.locationKey())
							.domainKey(key.domainKey())
							.contentKey(key.contentKey())
							.versionKey(update.vKey)
							.requestP2PConfiguration(
									new RequestP2PConfiguration(configuration
											.getReplicationFactor(), 5,
											configuration
													.getReplicationFactor()))
							.start();
					futureRemove.awaitUninterruptibly();
				} while (futureRemove.isSuccess());

				increaseForkAfterPutCounter();

				// exponential back off waiting
				while (true) {
					try {
						waitTime = waitTime + random.nextInt(1000);
						Thread.sleep(waitTime);
						break;
					} catch (InterruptedException e) {
						waitTime = 0;
						logger.error("Got interupted.", e);
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Update getAndUpdate(PeerDHT peer) throws IOException,
			ClassNotFoundException {
		while (true) {
			// fetch latest versions from the network, request also digest
			NavigableMap<Number640, Set<Number160>> versionTree;
			Map<Number640, Data> latestVersions;
			int counter = 0;
			while (true) {
				FutureGet futureGet = peer
						.get(key.locationKey())
						.domainKey(key.domainKey())
						.contentKey(key.contentKey())
						.getLatest()
						.withDigest()
						.requestP2PConfiguration(
								new RequestP2PConfiguration(3, 0, 3)).start();
				futureGet.awaitUninterruptibly();

				// get raw result from all contacted peers
				Map<PeerAddress, Map<Number640, Data>> rawData = futureGet
						.rawData();
				Map<PeerAddress, DigestResult> rawDigest = futureGet
						.rawDigest();

				// build the version tree from raw digest result;
				versionTree = Utils.buildVersionTree(rawDigest);

				// join all versions in one map
				latestVersions = Utils.getLatestVersions(rawData, id);

				// check if get was successful (first time can fail)
				if ((futureGet.isFailed() || latestVersions.isEmpty())
						&& !firstTime) {
					logger.warn("Couldn't get data. try #{}", counter++);
					if (counter > 2) {
						logger.warn("Loading of data failed after {} tries.",
								counter);
						// report it
						increaseConsistencyBreak();
						break;
					}
				} else {
					break;
				}
			}

			if (Utils.hasVersionDelay(latestVersions, versionTree)
					|| isDelayed(versionTree)) {
				logger.warn("Detected a version delay. versions = '{}'",
						Utils.getVersionNumbersFromMap(latestVersions));
				increaseDelayCounter();
				Utils.waitAMoment();
				continue;
			} else if (Utils.hasVersionForkAfterGet(latestVersions)) {
				logger.warn("Got a version fork. Merging. versions = '{}'",
						Utils.getVersionNumbersFromMap(latestVersions));
				return updateMerge(latestVersions);
			} else {
				Map<String, Integer> value;
				Number160 basedOnKey;

				if (latestVersions.isEmpty()) {
					value = new HashMap<String, Integer>();
					basedOnKey = Number160.ZERO;
				} else {
					// retrieve latest entry
					Entry<Number640, Data> lastEntry = latestVersions
							.entrySet().iterator().next();
					value = ((Map<String, Integer>) lastEntry.getValue()
							.object());
					basedOnKey = lastEntry.getKey().versionKey();
				}

				// update data
				if (value.containsKey(id)) {
					value.put(id, value.get(id) + 1);
				} else {
					value.put(id, 1);
				}

				// create new data wrapper
				Data data = new Data(value).addBasedOn(basedOnKey);
				// generate a new version key
				Number160 versionKey = Utils.generateVersionKey(basedOnKey,
						value.toString());

				return new Update(data, versionKey, false);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Update updateMerge(Map<Number640, Data> versionsToMerge)
			throws ClassNotFoundException, IOException {
		if (versionsToMerge == null || versionsToMerge.isEmpty()
				|| versionsToMerge.size() < 2) {
			throw new IllegalArgumentException(
					"Map with version to merge can't be null, empty or having only one entry.");
		}

		TreeMap<Number640, Data> sortedMap = new TreeMap<Number640, Data>(
				versionsToMerge);

		// merge maps together
		Map<String, Integer> mergedValue = new HashMap<String, Integer>();
		for (Number640 key : versionsToMerge.keySet()) {
			Map<String, Integer> value = (Map<String, Integer>) versionsToMerge
					.get(key).object();
			for (String id : value.keySet()) {
				if (mergedValue.containsKey(id)) {
					if (mergedValue.get(id) > value.get(id)) {
						mergedValue.put(id, mergedValue.get(id));
					} else {
						mergedValue.put(id, value.get(id));
					}
				} else {
					mergedValue.put(id, value.get(id));
				}
			}
		}

		// perform own write
		if (mergedValue.containsKey(id)) {
			mergedValue.put(id, mergedValue.get(id) + 1);
		} else {
			mergedValue.put(id, 1);
		}

		// create new data object
		Data data = new Data(mergedValue);
		// add all version keys as based on keys
		for (Number640 key : versionsToMerge.keySet()) {
			data.addBasedOn(key.versionKey());
		}
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey(sortedMap.lastEntry()
				.getKey().versionKey(), mergedValue.toString());

		return new Update(data, versionKey, true);
	}

	/**
	 * Checks if the latest version of the given digest is older than the cached
	 * version.
	 * 
	 * @param versionTree
	 *            digest containing all known versions
	 * @return <code>true</code> if cached version is newer, <code>false</code>
	 *         if not
	 */
	private boolean isDelayed(
			NavigableMap<Number640, Set<Number160>> versionTree) {
		// get latest version, and store it if newer
		if (!versionTree.isEmpty()) {
			Number160 latestVersion = versionTree.lastKey().versionKey();
			if (latestVersion.compareTo(memorizedVersionKey) < 0) {
				logger.warn(
						"Detected a later version. memorizedVersion = '{}' latestVersion='{}'",
						memorizedVersionKey, latestVersion);
				memorizedVersionKey = latestVersion;
				return true;
			}
		}
		return false;
	}

	private class Update {

		private final Data data;
		private final Number160 vKey;
		private final boolean isMerge;

		public Update(Data data, Number160 vKey, boolean isMerge) {
			this.data = data;
			this.vKey = vKey;
			this.isMerge = isMerge;
		}

	}

}
