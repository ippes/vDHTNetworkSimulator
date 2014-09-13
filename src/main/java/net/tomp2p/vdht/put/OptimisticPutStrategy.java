package net.tomp2p.vdht.put;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
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
 * Put strategy following the optimistic vDHT approach.
 * 
 * @author Seppi
 */
public final class OptimisticPutStrategy extends PutStrategy {

	private final Logger logger = LoggerFactory
			.getLogger(OptimisticPutStrategy.class);

	public static final String PUT_STRATEGY_NAME = "optimistic";

	private final Configuration configuration;
	private final int replicationFactor;
	private final int maxVersions;

	// cached data
	private NavigableMap<Number640, Set<Number160>> versionTree = new TreeMap<Number640, Set<Number160>>();
	private NavigableMap<Number640, Data> cachedVersions = new TreeMap<Number640, Data>();
	private NavigableSet<Number160> removedVersions = new TreeSet<Number160>();

	private boolean firstTime = true;
	private final Random random = new Random();

	private final int putFailedLimit = 3;
	private final int getFailedLimit = 3;
	private final int forkLimit = 5;
	private final int delayLimit = 3;

	public OptimisticPutStrategy(String id, Number480 key, Result result,
			Configuration configuration) {
		super(id, key, result);
		this.configuration = configuration;
		this.replicationFactor = configuration.getReplicationFactor();
		this.maxVersions = configuration.getMaxVersions();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// repeat as long as a version has no delays nor forks
		int forkCounter = 0;
		int forkWaitTime = random.nextInt(1000) + 1000;
		while (true) {
			// get and update value from network
			Update update = getAndUpdate(peer);

			// set time to live
			Data updatedData = update.data;
			if (configuration.getPutTTLInSeconds() > 0) {
				updatedData.ttlSeconds(configuration.getPutTTLInSeconds());
			}

			// put updated version into network
			FuturePut futurePut;
			int putCounter = 0;
			int putWaitTime = random.nextInt(500) + 500;
			while (true) {
				// put updated version into network
				futurePut = put(peer, update.vKey, updatedData);

				// analyze put
				if (futurePut.isFailed()) {
					if (putCounter > putFailedLimit) {
						logger.warn("Put failed after {} tries. reason = '{}'",
								putCounter, futurePut.failedReason());
						return;
					} else {
						logger.warn("Put failed. Try #{}. Retrying. reason = '{}'",
								putCounter++, futurePut.failedReason());
						// exponential back off waiting
						Thread.sleep(putWaitTime);
						putWaitTime = putWaitTime * 2;
					}
				} else {
					break;
				}
			}

			logger.debug(
					"Put. value = '{}', write counter = '{}' version = '{}'",
					update.data.object(), getWriteCounter(),
					update.vKey.timestamp());

			// check for any version forks
			if (!Utils.hasVersionForkAfterPut(futurePut.rawResult())
					|| forkCounter >= forkLimit) {
				if (forkCounter >= forkLimit) {
					logger.warn("Ignoring fork after {} rejects and retries.",
							forkCounter);
				}

				// cache put
				versionTree.put(new Number640(key, update.vKey),
						new HashSet<Number160>(update.data.basedOnSet()));
				Utils.removeOutdatedVersions(versionTree, maxVersions);
				removedVersions.remove(update.vKey);
				cachedVersions
						.put(new Number640(key, update.vKey), update.data);
				Utils.removeOutdatedVersions(cachedVersions, maxVersions);

				// check for consistency breaks
				if (!firstTime
						&& ((HashMap<String, Integer>) update.data.object())
								.get(id) == 1) {
					increaseConsistencyBreak();
				}

				if (!update.isMerge) {
					firstTime = false;
				}
				break;
			} else {
				logger.warn("Version fork after put detected. Rejecting and retrying put.");

				// reject put with tombstones
				Data tombstone = new Data().deleted(true);
				if (configuration.getPutTTLInSeconds() > 0) {
					tombstone.ttlSeconds(configuration.getPutTTLInSeconds());
				}
				put(peer, update.vKey, tombstone);
				removedVersions.add(update.vKey);

				// protocol events
				if (update.isMerge) {
					decreaseMergeCounter();
				} else {
					decreaseWriteCounter();
				}
				forkCounter++;
				increaseForkAfterPutCounter();

				// exponential back off waiting
				Thread.sleep(forkWaitTime);
				forkWaitTime = forkWaitTime * 2;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Update getAndUpdate(PeerDHT peer) throws IOException,
			ClassNotFoundException, InterruptedException {
		// repeat till no version delays occur and forks get resolved
		int delayCounter = 0;
		int delayWaitTime = random.nextInt(1000) + 1000;
		int forkCounter = 0;
		int forkWaitTime = random.nextInt(1000) + 1000;
		NavigableMap<Number640, Data> fetchedVersions;
		while (true) {
			// fetch latest versions from the network, request also digest
			int getCounter = 0;
			int getWaitTime = random.nextInt(1000) + 1000;
			while (true) {
				// load latest data
				FutureGet futureGet = get(peer);

				// get raw result from all contacted peers
				Map<PeerAddress, Map<Number640, Data>> rawData = futureGet
						.rawData();
				Map<PeerAddress, DigestResult> rawDigest = futureGet
						.rawDigest();

				// build and merge the version tree from raw digest result;
				versionTree.putAll(Utils.buildVersionTree(rawDigest));
				Utils.removeOutdatedVersions(versionTree, maxVersions);

				// join all freshly loaded versions in one map
				fetchedVersions = Utils.buildVersions(rawData);
				for (Iterator<Number640> it = fetchedVersions.keySet()
						.iterator(); it.hasNext();) {
					Number640 key = it.next();
					if (fetchedVersions.get(key).isDeleted()) {
						removedVersions.add(key.versionKey());
						it.remove();
					}
				}
				for (Iterator<Number640> it = fetchedVersions.keySet()
						.iterator(); it.hasNext();) {
					Number640 key = it.next();
					if (removedVersions.contains(key.versionKey())) {
						logger.warn(
								"Got an already removed version. Rejecting again. version = '{}'",
								key.versionKey().timestamp());
						// reject put again with tombstones
						Data tombstone = new Data().deleted(true);
						if (configuration.getPutTTLInSeconds() > 0) {
							tombstone.ttlSeconds(configuration.getPutTTLInSeconds());
						}
						put(peer, key.versionKey(), tombstone);
						it.remove();
					}
				}
				Utils.removeOutdatedVersions(fetchedVersions, maxVersions);

				// check if get was successful (first time can fail)
				if ((futureGet.isFailed() || fetchedVersions.isEmpty())
						&& !firstTime) {
					if (getCounter > getFailedLimit) {
						logger.warn(
								"Loading of data failed after {} tries. reason = '{}' direct hits = '{}' potential hits = '{}'",
								getCounter, futureGet.failedReason(), futureGet
										.futureRouting().directHits(),
								futureGet.futureRouting().potentialHits());
						break;
					} else {
						logger.warn(
								"Couldn't get data. Try #{}. Retrying. reason = '{}' direct hits = '{}' potential hits = '{}' fetched versions = '{}'",
								getCounter++, futureGet.failedReason(),
								futureGet.futureRouting().directHits(),
								futureGet.futureRouting().potentialHits(),
								fetchedVersions);
						// maintenance: reput latest versions
						for (Number640 version : cachedVersions.keySet()) {
							put(peer,
									version.versionKey(),
									cachedVersions.get(version));
						}
						// exponential back off waiting
						Thread.sleep(getWaitTime);
						getWaitTime = getWaitTime * 2;
					}
				} else {
					break;
				}
			}

			// check if version delays or forks occurred
			if (Utils.hasVersionDelay(fetchedVersions, versionTree)
					&& delayCounter < delayLimit) {
				logger.warn("Detected a version delay. versions = '{}'",
						Utils.getVersionNumbersFromMap(fetchedVersions));

				// do statistics
				delayCounter++;
				increaseDelayCounter();

				// do some reput maintenance, consider only latest delayed
				// versions
				SortedMap<Number640, Data> toRePut = new TreeMap<Number640, Data>(
						cachedVersions).tailMap(fetchedVersions.firstKey(),
						true);
				logger.debug("Reputting delayed versions. reputs = '{}'",
						Utils.getVersionNumbersFromMap(toRePut));
				for (Number640 delayed : toRePut.keySet()) {
					put(peer, delayed.versionKey(), toRePut.get(delayed));
				}

				// exponential back off waiting
				Thread.sleep(delayWaitTime);
				delayWaitTime = delayWaitTime * 2;

				continue;
			}

			// check for version fork
			if (fetchedVersions.size() > 1 && delayCounter < delayLimit
					&& forkCounter >= forkLimit) {
				logger.warn("Got a version fork. Merging. versions = '{}'",
						Utils.getVersionNumbersFromMap(fetchedVersions));
				return updateMerge(fetchedVersions);
			} else if (fetchedVersions.size() > 1 && delayCounter < delayLimit) {
				logger.warn("Got a version fork. Waiting. versions = '{}'",
						Utils.getVersionNumbersFromMap(fetchedVersions));

				// do statistics
				forkCounter++;
				increaseForkAfterGetCounter();

				// exponential back off waiting
				Thread.sleep(forkWaitTime);
				forkWaitTime = forkWaitTime * 2;

				continue;
			} else {
				if (delayCounter >= delayLimit) {
					logger.warn("Ignoring delay after {} retries.",
							delayCounter);
				}

				Map<String, Integer> value;
				Number160 basedOnKey;
				if (fetchedVersions.isEmpty()) {
					value = new HashMap<String, Integer>();
					basedOnKey = Number160.ZERO;
				} else {
					// retrieve latest entry
					Entry<Number640, Data> lastEntry = fetchedVersions
							.lastEntry();
					value = ((Map<String, Integer>) lastEntry.getValue()
							.object());
					basedOnKey = lastEntry.getKey().versionKey();
					logger.debug("Got. value = '{}' version = '{}'", value,
							basedOnKey.timestamp());
				}

				// update data
				if (value.containsKey(id)) {
					value.put(id, value.get(id) + 1);
				} else {
					value.put(id, 1);
				}
				increaseWriteCounter();

				// create a new updated wrapper
				Data data = new Data(value).addBasedOn(basedOnKey);
				// generate a new version key
				Number160 versionKey = Utils.generateVersionKey(basedOnKey,
						value.toString() + UUID.randomUUID());
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

		// create new data object
		Data data = new Data(mergedValue);
		// add all version keys as based on keys
		for (Number640 key : versionsToMerge.keySet()) {
			data.addBasedOn(key.versionKey());
		}
		// generate a new version key
		Number160 versionKey = Utils.generateVersionKey(sortedMap.lastEntry()
				.getKey().versionKey(), mergedValue.toString());

		increaseMergeCounter();

		return new Update(data, versionKey, true);
	}

	private FutureGet get(PeerDHT peer) {
		FutureGet futureGet = peer
				.get(key.locationKey())
				.domainKey(key.domainKey())
				.contentKey(key.contentKey())
				.getLatest()
				.withDigest()
				.fastGet(false)
				.requestP2PConfiguration(
						new RequestP2PConfiguration(replicationFactor, 50, 0))
				.start();
		futureGet.awaitUninterruptibly();
		return futureGet;
	}

	private FuturePut put(PeerDHT peer, Number160 vKey, Data data) {
		FuturePut futurePut = peer
				.put(key.locationKey())
				.data(key.contentKey(), data)
				.domainKey(key.domainKey())
				.versionKey(vKey)
				.requestP2PConfiguration(
						new RequestP2PConfiguration(replicationFactor, 50, 0))
				.start();
		futurePut.awaitUninterruptibly();
		return futurePut;
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

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Integer> getLatest() {
		try {
			return (Map<String, Integer>) cachedVersions.lastEntry().getValue()
					.object();
		} catch (Exception e) {
			logger.warn("Couldn't return latest version.");
			return null;
		}
	}

}
