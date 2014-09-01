package net.tomp2p.vdht.put;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
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
	private final int replicationFactor;

	// cached data
	// TODO get rid of versionTree, cachedVersions has same data
	private NavigableMap<Number640, Set<Number160>> versionTree = new TreeMap<Number640, Set<Number160>>();
	private NavigableMap<Number640, Data> cachedVersions = new TreeMap<Number640, Data>();

	private boolean firstTime = true;
	private Random random = new Random();

	// limit constants
	private final int putFailedLimit = 3;
	private final int getFailedLimit = 3;
	private final int forkLimit = 5;
	private final int delayLimit = 3;

	public PesimisticPutStrategy(String id, Number480 key, Result result,
			Configuration configuration) {
		super(id, key, result);
		this.configuration = configuration;
		this.replicationFactor = configuration.getReplicationFactor();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void getUpdateAndPut(PeerDHT peer) throws Exception {
		// repeat as long as a version can be confirmed
		int forkCounter = 0;
		int forkWaitTime = random.nextInt(1000) + 1000;
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
			int putCounter = 0;
			int putWaitTime = random.nextInt(500) + 500;
			while (true) {
				futurePut = put(peer, update.vKey, updatedData);
				// analyze put
				if (futurePut.isFailed()) {
					logger.warn("Put failed. Try #{}. Retrying.", putCounter++);
					if (putCounter > putFailedLimit) {
						logger.warn("Put failed after {} tries. reason = '{}'",
								putCounter, futurePut.failedReason());
						// peer is broken return with no action
						return;
					} else {
						// exponential back off waiting
						Thread.sleep(putWaitTime);
						putWaitTime = putWaitTime * 2;
					}
				} else {
					break;
				}
			}

			logger.debug("Put. value = '{}' version = '{}'",
					update.data.object(), update.vKey.timestamp());

			// check for any version forks
			if (!Utils.hasVersionForkAfterPut(futurePut.rawResult())
					|| forkCounter >= forkLimit) {
				if (forkCounter >= forkLimit) {
					logger.warn("Ignoring fork after {} rejects and retries.",
							forkCounter);
				}

				// confirm put
				Data data = new Data();
				if (configuration.getPutTTLInSeconds() > 0) {
					data.ttlSeconds(configuration.getPutTTLInSeconds());
				}
				FuturePut futurePutConfirm = peer
						.put(key.locationKey())
						.domainKey(key.domainKey())
						.data(key.contentKey(), data)
						.versionKey(update.vKey)
						.putConfirm()
						.requestP2PConfiguration(
								new RequestP2PConfiguration(replicationFactor,
										replicationFactor * 2,
										replicationFactor * 2 - 2)).start();
				futurePutConfirm.awaitUninterruptibly();

				// cache put
				versionTree.put(new Number640(key, update.vKey),
						new HashSet<Number160>(update.data.basedOnSet()));
				cachedVersions
						.put(new Number640(key, update.vKey), update.data);
				while (cachedVersions.firstKey().versionKey().timestamp()
						+ configuration.getMaxVersions() <= cachedVersions
						.lastKey().versionKey().timestamp()) {
					cachedVersions.pollFirstEntry();
				}

				// check for consistency breaks
				if (!firstTime
						&& ((HashMap<String, Integer>) update.data.object())
								.get(id) == 1) {
					increaseConsistencyBreak();
				}

				// do statistics
				if (update.isMerge) {
					increaseMergeCounter();
				} else {
					increaseWriteCounter();
					// set flag
					firstTime = false;
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
									new RequestP2PConfiguration(
											replicationFactor,
											replicationFactor * 2,
											replicationFactor * 2 - 2)).start();
					futureRemove.awaitUninterruptibly();
				} while (futureRemove.isSuccess());

				// do statistics
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
		int delayCounter = 0;
		int delayWaitTime = random.nextInt(1000) + 1000;
		while (true) {
			// fetch latest versions from the network, request also digest
			NavigableMap<Number640, Data> latestVersions;
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

				// build the version tree from raw digest result;
				versionTree.putAll(Utils.buildVersionTree(rawDigest));
				if (!versionTree.isEmpty()) {
					// remove all clearly out dated versions
					while (versionTree.firstKey().versionKey().timestamp()
							+ configuration.getMaxVersions() <= versionTree
							.lastKey().versionKey().timestamp()) {
						versionTree.pollFirstEntry();
					}
				}

				// join all versions in one map
				latestVersions = Utils.getLatestVersions(rawData, id);
				if (!latestVersions.isEmpty()) {
					// remove all clearly out dated versions
					while (latestVersions.firstKey().versionKey().timestamp()
							+ configuration.getMaxVersions() <= latestVersions
							.lastKey().versionKey().timestamp()) {
						latestVersions.pollFirstEntry();
					}
					cachedVersions.putAll(latestVersions);
				}

				// check if get was successful (first time can fail)
				if ((futureGet.isFailed() || latestVersions.isEmpty())
						&& !firstTime) {
					logger.warn("Couldn't get data. Try #{}. Retrying.",
							getCounter++);
					if (getCounter > getFailedLimit) {
						logger.warn(
								"Loading of data failed after {} tries. reason = '{}'",
								getCounter, futureGet.failedReason());
						break;
					} else {
						// maintenance: reput latest versions
						for (Number640 version : cachedVersions.keySet()) {
							put(peer,
									version.versionKey(),
									cachedVersions.get(version).prepareFlag(
											true));
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
			if (Utils.hasVersionDelay(latestVersions, versionTree)
					&& delayCounter < delayLimit) {
				logger.warn(
						"Detected a version delay. versions = '{}' history = '{}'",
						Utils.getVersionNumbersFromMap(latestVersions),
						Utils.getVersionNumbersFromMap2(versionTree));

				// do statistics
				delayCounter++;
				increaseDelayCounter();

				// do some reput maintenance, consider only latest delayed
				// versions
				SortedMap<Number640, Data> toRePut = new TreeMap<Number640, Data>(
						cachedVersions)
						.tailMap(latestVersions.firstKey(), true);
				logger.debug("Reputting delayed versions. reputs = '{}'",
						Utils.getVersionNumbersFromMap(toRePut));
				for (Number640 delayed : toRePut.keySet()) {
					put(peer, delayed.versionKey(), toRePut.get(delayed)
							.prepareFlag(false));
				}

				// exponential back off waiting
				Thread.sleep(delayWaitTime);
				delayWaitTime = delayWaitTime * 2;

				continue;
			} else if (Utils.hasVersionForkAfterGet(latestVersions,
					configuration.getMaxVersions())
					&& delayCounter < delayLimit) {
				logger.warn(
						"Got a version fork. Merging. versions = '{}' history = '{}'",
						Utils.getVersionNumbersFromMap(latestVersions),
						Utils.getVersionNumbersFromMap2(versionTree));
				return updateMerge(latestVersions);
			} else {
				if (delayCounter >= delayLimit) {
					logger.warn("Ignoring delay after {} retries.",
							delayCounter);
				}

				Map<String, Integer> value;
				Number160 basedOnKey;
				if (cachedVersions.isEmpty()) {
					value = new HashMap<String, Integer>();
					basedOnKey = Number160.ZERO;
				} else {
					// retrieve latest entry
					Entry<Number640, Data> lastEntry = cachedVersions
							.lastEntry();
					value = ((Map<String, Integer>) lastEntry.getValue()
							.object());
					basedOnKey = lastEntry.getKey().versionKey();
				}

				logger.debug("Got. value = '{}' version = '{}'", value,
						basedOnKey.timestamp());

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

	private FutureGet get(PeerDHT peer) {
		FutureGet futureGet = peer
				.get(key.locationKey())
				.domainKey(key.domainKey())
				.contentKey(key.contentKey())
				.getLatest()
				.withDigest()
				.requestP2PConfiguration(
						new RequestP2PConfiguration(replicationFactor - 1, 0, 1))
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
						new RequestP2PConfiguration(replicationFactor - 1, 0, 1))
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

}
