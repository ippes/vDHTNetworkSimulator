package net.tomp2p.vdht.put;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
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

public final class OptimisticPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory.getLogger(OptimisticPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "optimistic";

	private final int putTTLInSeconds;

	private Number160 memorizedVersionKey = Number160.ZERO;

	private int versionForkAfterPut = 0;
	private int versionDelay = 0;
	private int versionForkAfterGetWait = 0;
	private int versionForkAfterGetMerge = 0;

	public OptimisticPutStrategy(String id, Number480 key) throws IOException {
		super(id, key);
		this.putTTLInSeconds = Configuration.getPutTTLInSeconds();
		logger.trace("put ttl in seconds = '{}'", putTTLInSeconds);
	}

	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// repeat as long as a version can be confirmed
		while (true) {
			// get and update value from network
			Result result = getAndUpdate(peer);

			// set time to live
			Data updatedData = result.getData();
			updatedData.ttlSeconds(putTTLInSeconds);

			// put updated version into network
			FuturePut futurePut = peer.put(key.locationKey()).data(key.contentKey(), updatedData)
					.domainKey(key.domainKey()).versionKey(result.getVersionKey()).start();
			futurePut.awaitUninterruptibly();

			logger.debug("Put. version = '{}'", result.getVersionKey());

			// check for any version forks
			if (!Utils.hasVersionForkAfterPut(futurePut.rawResult())) {
				// store version key
				if (result.getVersionKey().compareTo(memorizedVersionKey) < 0) {
					memorizedVersionKey = result.getVersionKey();
				}

				putCounter++;

				logger.trace("No version forks detected. key = '{}' id = '{}'", key, id);
				break;
			} else {
				// reject put
				FuturePut futurePutConfirm = peer.put(key.locationKey()).domainKey(key.domainKey())
						.data(key.contentKey(), new Data()).versionKey(result.getVersionKey()).putReject()
						.start();
				futurePutConfirm.awaitUninterruptibly();

				versionForkAfterPut++;

				logger.warn("Version fork after put detected. Retry put.");
			}
		}
	}

	private long time = 0;

	private Result getAndUpdate(PeerDHT peer) throws IOException, ClassNotFoundException {
		time = System.currentTimeMillis();
		while (true) {
			// fetch latest versions from the network, request also digest
			FutureGet futureGet = peer.get(key.locationKey()).domainKey(key.domainKey())
					.contentKey(key.contentKey()).getLatest().withDigest().start();
			futureGet.awaitUninterruptibly();

			// get raw result from all contacted peers
			Map<PeerAddress, Map<Number640, Data>> rawData = futureGet.rawData();
			logger.debug("Got. versions = '{}'", Utils.getVersionKeysFromPeers(rawData));
			Map<PeerAddress, DigestResult> rawDigest = futureGet.rawDigest();

			// build the version tree from raw digest result;
			NavigableMap<Number640, Set<Number160>> versionTree = Utils.buildVersionTree(rawDigest);

			// join all versions in one map
			Map<Number640, Data> latestVersions = Utils.getLatestVersions(rawData);

			if (Utils.hasVersionForkAfterGet(latestVersions) && time + 1000 < System.currentTimeMillis()) {
				logger.warn(
						"Got a version fork. Timeout expired. Merging. versions = '{}'  latestVersions = '{}'",
						Utils.getVersionKeysFromPeers(rawData), Utils.getVersionKeysFromMap(latestVersions));
				versionForkAfterGetWait++;
				return updateMerge(latestVersions);
			} else if (Utils.hasVersionForkAfterGet(latestVersions)) {
				logger.warn("Got a version fork. Waiting. versions = '{}'",
						Utils.getVersionKeysFromMap(latestVersions));
				versionForkAfterGetMerge++;
				Utils.waitAMoment();
				continue;
			} else if (Utils.hasVersionDelay(latestVersions, versionTree) || isDelayed(versionTree)) {
				logger.warn("Detected a version delay. versions = '{}'",
						Utils.getVersionKeysFromMap(latestVersions));
				versionDelay++;
				Utils.waitAMoment();
				continue;
			} else {
				if (latestVersions.isEmpty()) {
					logger.debug("Received an empty data map. key = '{}' id = '{}'", key, id);
					// reset value
					Data data = new Data(id).addBasedOn(Number160.ZERO);
					// generate a new version key
					Number160 versionKey = Utils.generateVersionKey(Number160.ZERO, id);
					return new Result(data, versionKey);
				} else {
					// retrieve latest entry
					Entry<Number640, Data> lastEntry = latestVersions.entrySet().iterator().next();
					// update data
					return updateData(lastEntry);
				}
			}
		}
	}

	private Result updateMerge(Map<Number640, Data> versionsToMerge) throws ClassNotFoundException,
			IOException {
		if (versionsToMerge == null || versionsToMerge.isEmpty() || versionsToMerge.size() < 2) {
			throw new IllegalArgumentException(
					"Map with version to merge can't be null, empty or having only one entry.");
		}

		TreeMap<Number640, Data> sortedMap = new TreeMap<Number640, Data>(versionsToMerge);
		Data latestVersion = sortedMap.lastEntry().getValue();

		// search longest common substring
		String commonString = (String) latestVersion.object();
		for (Number640 key : versionsToMerge.keySet()) {
			String value = (String) versionsToMerge.get(key).object();
			for (int i = Math.min(commonString.length(), value.length()); i >= 0; i--) {
				String subString = commonString.substring(0, i);
				if (value.startsWith(subString)) {
					commonString = subString;
					break;
				}
			}
		}

		// append all not common substrings
		String mergedValue = commonString;
		for (Number640 key : versionsToMerge.keySet()) {
			String value = (String) versionsToMerge.get(key).object();
			mergedValue += value.substring(commonString.length(), value.length());
		}

		// append own id
		mergedValue += id;

		// create new data object
		Data data = new Data(mergedValue);
		// add all version keys as based on keys
		for (Number640 key : versionsToMerge.keySet()) {
			data.addBasedOn(key.versionKey());
		}
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey(sortedMap.lastEntry().getKey().versionKey(),
				mergedValue);

		return new Result(data, versionKey);
	}

	/**
	 * Updates given data (appending id). Generates a new version key.
	 * 
	 * @param entry
	 *            data to update
	 * @return updated data and it's version key
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private Result updateData(Entry<Number640, Data> entry) throws ClassNotFoundException, IOException {
		// update data
		String value = ((String) entry.getValue().object()) + id;
		// create a new updated wrapper
		Data data = new Data(value);
		// set based on key
		data.addBasedOn(entry.getKey().versionKey());
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey(entry.getKey().versionKey(), value);
		return new Result(data, versionKey);
	}

	/**
	 * Checks if the latest version of the given digest is older than the cached version.
	 * 
	 * @param versionTree
	 *            digest containing all known versions
	 * @return <code>true</code> if cached version is newer, <code>false</code> if not
	 */
	private boolean isDelayed(NavigableMap<Number640, Set<Number160>> versionTree) {
		// get latest version, and store it if newer
		if (!versionTree.isEmpty()) {
			Number160 latestVersion = versionTree.lastKey().versionKey();
			if (latestVersion.compareTo(memorizedVersionKey) < 0) {
				logger.warn("Detected a later version. memorizedVersion = '{}' latestVersion='{}'",
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
		logger.debug("version forks after get and merge = '{}'", versionForkAfterGetMerge);
		logger.debug("version forks after get and wait = '{}'", versionForkAfterGetWait);
	}

}
