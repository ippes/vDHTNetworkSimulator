package net.tomp2p.vdht.put;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.FutureRemove;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Pair;
import net.tomp2p.vdht.Configuration;
import net.tomp2p.vdht.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptimisticPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory
			.getLogger(OptimisticPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "optimistic";

	private final Configuration configuration;

	private Number160 memorizedVersionKey = Number160.ZERO;

	private int versionForkAfterPut = 0;
	private int versionDelay = 0;
	private int versionForkAfterGetWait = 0;
	private int versionForkAfterGetMerge = 0;

	public OptimisticPutStrategy(String id, Number480 key,
			Configuration configuration) {
		super(id, key);
		this.configuration = configuration;
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// repeat as long as a version can be confirmed
		while (true) {
			// get and update value from network
			Pair<Data, Number160> result = getAndUpdate(peer);

			// set time to live
			Data updatedData = result.element0();
			if (configuration.getPutTTLInSeconds() > 0) {
				updatedData.ttlSeconds(configuration.getPutTTLInSeconds());
			}

			// put updated version into network
			FuturePut futurePut = peer.put(key.locationKey())
					.data(key.contentKey(), updatedData)
					.domainKey(key.domainKey()).versionKey(result.element1())
					.start();
			futurePut.awaitUninterruptibly();

			logger.debug("Put. value = '{}', putCounter = '{}' version = '{}'", result
					.element0().object(), putCounter, result.element1().timestamp());

			// check for any version forks
			if (!Utils.hasVersionForkAfterPut(futurePut.rawResult())) {
				// store version key
				if (result.element1().compareTo(memorizedVersionKey) < 0) {
					memorizedVersionKey = result.element1();
				}
				break;
			} else {
				// reject put
				FutureRemove futureRemove;
				do {
					futureRemove = peer.remove(key.locationKey())
							.domainKey(key.domainKey())
							.contentKey(key.contentKey())
							.versionKey(result.element1()).start();
					futureRemove.awaitUninterruptibly();
					System.err.println("hallo");
				} while (futureRemove.isSuccess());

				putCounter--;
				versionForkAfterPut++;

				logger.warn("Version fork after put detected. Retry put.");
			}
		}
	}

	private long time = 0;

	@SuppressWarnings("unchecked")
	private Pair<Data, Number160> getAndUpdate(PeerDHT peer)
			throws IOException, ClassNotFoundException {
		time = System.currentTimeMillis();
		while (true) {
			// fetch latest versions from the network, request also digest
			FutureGet futureGet = peer.get(key.locationKey())
					.domainKey(key.domainKey()).contentKey(key.contentKey())
					.getLatest().withDigest().start();
			futureGet.awaitUninterruptibly();

			// get raw result from all contacted peers
			Map<PeerAddress, Map<Number640, Data>> rawData = futureGet
					.rawData();
			Map<PeerAddress, DigestResult> rawDigest = futureGet.rawDigest();

			// build the version tree from raw digest result;
			NavigableMap<Number640, Set<Number160>> versionTree = Utils
					.buildVersionTree(rawDigest);

			// join all versions in one map
			Map<Number640, Data> latestVersions = Utils
					.getLatestVersions(rawData);

			if (Utils.hasVersionForkAfterGet(latestVersions)
					&& time + 3000 < System.currentTimeMillis()) {
				logger.warn(
						"Got a version fork. Timeout expired. Merging. latestVersions = '{}'",
						Utils.getVersionNumbersFromMap(latestVersions));
				versionForkAfterGetWait++;
				return updateMerge(latestVersions);
			} else if (Utils.hasVersionForkAfterGet(latestVersions)) {
				logger.warn("Got a version fork. Waiting. versions = '{}'",
						Utils.getVersionNumbersFromMap(latestVersions));
				versionForkAfterGetMerge++;
				Utils.waitAMoment();
				continue;
			} else if (Utils.hasVersionDelay(latestVersions, versionTree)
					|| isDelayed(versionTree)) {
				logger.warn("Detected a version delay. versions = '{}'",
						Utils.getVersionNumbersFromMap(latestVersions));
				versionDelay++;
				Utils.waitAMoment();
				continue;
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
				putCounter++;

				// create a new updated wrapper
				Data data = new Data(value).addBasedOn(basedOnKey);
				// generate a new version key
				Number160 versionKey = Utils.generateVersionKey(basedOnKey,
						value.toString());
				return new Pair<Data, Number160>(data, versionKey);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Pair<Data, Number160> updateMerge(
			Map<Number640, Data> versionsToMerge)
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

		putCounter++;

		// create new data object
		Data data = new Data(mergedValue);
		// add all version keys as based on keys
		for (Number640 key : versionsToMerge.keySet()) {
			data.addBasedOn(key.versionKey());
		}
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey(sortedMap.lastEntry()
				.getKey().versionKey(), mergedValue.toString());

		return new Pair<Data, Number160>(data, versionKey);
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

	@Override
	public void printResults() {
		logger.debug("version delays = '{}'", versionDelay);
		logger.debug("version forks after put = '{}'", versionForkAfterPut);
		logger.debug("version forks after get and merge = '{}'",
				versionForkAfterGetMerge);
		logger.debug("version forks after get and wait = '{}'",
				versionForkAfterGetWait);
	}

}
