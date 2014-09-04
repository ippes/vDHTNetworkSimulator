package net.tomp2p.vdht;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.dht.StorageLayer.PutStatus;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

	private final static Logger logger = LoggerFactory.getLogger(Utils.class);

	public static Number160 generateVersionKey(Number160 basedOnKey,
			String value) {
		// increase counter
		long counter = basedOnKey.timestamp() + 1;
		// create new version key based on increased counter and hash
		return new Number160(counter, Number160.createHash(value).number96());
	}

	/**
	 * Checks given peer result map for any {@link PutStatus#VERSION_FORK}
	 * response.
	 * 
	 * @param dataMap
	 *            raw result from all contacted peers
	 * @return <code>true</code> if a version fork occurred,
	 *         <code>false<code/> if not
	 * @throws IllegalStateException
	 */
	public static boolean hasVersionForkAfterPut(
			Map<PeerAddress, Map<Number640, Byte>> dataMap)
			throws IllegalStateException {
		if (dataMap == null || dataMap.isEmpty()) {
			return false;
		}

		// go through all responding peers
		for (PeerAddress peerAddress : dataMap.keySet()) {
			Map<Number640, Byte> putResult = dataMap.get(peerAddress);
			if (putResult.size() != 1) {
				throw new IllegalStateException(
						String.format(
								"Received wrong sized data map. peerAddress = '%s' size = '%s'",
								peerAddress, putResult.size()));
			} else {
				Entry<Number640, Byte> result = new TreeMap<Number640, Byte>(
						putResult).firstEntry();
				if (result.getValue().intValue() == PutStatus.OK.ordinal()) {
					continue;
				} else if (result.getValue().intValue() == PutStatus.VERSION_FORK
						.ordinal()) {
					// contains a version fork
					return true;
				} else {
					if (result.getValue().intValue() > 0) {
						logger.error(
								"Received not handled put status as result. peerAddress = '{}' putStatus = '{}'",
								peerAddress, PutStatus.values()[result
										.getValue().intValue()]);
					} else {
						// ignore this peer
						// logger.warn(
						// "Received unkown put status as result. peerAddress = '{}' putStatus = '{}'",
						// peerAddress, result.getValue().intValue());
					}
				}
			}
		}
		return false;
	}

	/**
	 * Checks given peer result map if a peer has responded with more than one
	 * version.
	 * 
	 * @param latestVersions
	 *            raw result from all contacted peers
	 * @return <code>true</code> if a version fork occurred,
	 *         <code>false<code/> if not
	 * @throws IllegalStateException
	 */
	public static boolean hasVersionForkAfterGet(
			NavigableMap<Number640, Set<Number160>> versions) {
		NavigableMap<Number640, Set<Number160>> latest = getLatest2(versions);
		if (latest.size() > 1) {
			return true;
		} else {
			return false;
		}
	}

	public static NavigableMap<Number640, Set<Number160>> getLatest2(
			NavigableMap<Number640, Set<Number160>> versions) {
		// delete all predecessors
		NavigableMap<Number640, Set<Number160>> tmp = new TreeMap<Number640, Set<Number160>>(
				versions);
		NavigableMap<Number640, Set<Number160>> result = new TreeMap<Number640, Set<Number160>>();
		while (!tmp.isEmpty()) {
			// first entry is a latest version
			Entry<Number640, Set<Number160>> latest = tmp.lastEntry();
			// store in results list
			result.put(latest.getKey(), latest.getValue());
			// delete all predecessors of latest entry
			deletePredecessors2(latest.getKey(), tmp);
		}
		return result;
	}

	private static void deletePredecessors2(Number640 key,
			NavigableMap<Number640, Set<Number160>> sortedMap) {
		Set<Number160> basedOnSet = sortedMap.remove(key);
		// check if set has been already deleted
		if (basedOnSet == null) {
			return;
		}
		// check if version is initial version
		if (basedOnSet.isEmpty()) {
			return;
		}
		// remove all predecessor versions recursively
		for (Number160 basedOnKey : basedOnSet) {
			deletePredecessors2(new Number640(key.locationDomainAndContentKey(),
					basedOnKey), sortedMap);
		}
	}

	public static NavigableMap<Number640, Data> getLatest(
			NavigableMap<Number640, Data> versions) {
		// delete all predecessors
		NavigableMap<Number640, Data> tmp = new TreeMap<Number640, Data>(
				versions);
		NavigableMap<Number640, Data> result = new TreeMap<Number640, Data>();
		while (!tmp.isEmpty()) {
			// first entry is a latest version
			Entry<Number640, Data> latest = tmp.lastEntry();
			// store in results list
			result.put(latest.getKey(), latest.getValue());
			// delete all predecessors of latest entry
			deletePredecessors(latest.getKey(), tmp);
		}
		return result;
	}

	private static void deletePredecessors(Number640 key,
			NavigableMap<Number640, Data> sortedMap) {
		Data version = sortedMap.remove(key);
		// check if set has been already deleted
		if (version == null) {
			return;
		}
		// check if version is initial version
		if (version.basedOnSet().isEmpty()) {
			return;
		}
		// remove all predecessor versions recursively
		for (Number160 basedOnKey : version.basedOnSet()) {
			deletePredecessors(new Number640(key.locationDomainAndContentKey(),
					basedOnKey), sortedMap);
		}
	}

	/**
	 * Checks if peers responded with out dated versions. If a version (in the
	 * digest history) is basing on a returned latest version we have a version
	 * delay.
	 * 
	 * @param latestVersions
	 *            raw result from all contacted peers
	 * @param versionTree
	 *            digest history
	 * @return <code>true</code> if a version delay occurred, <code>false</code>
	 *         if not
	 */
	public static boolean hasVersionDelay(Map<Number640, Data> latestVersions,
			Map<Number640, Set<Number160>> versionTree) {
		for (Number640 version : versionTree.keySet()) {
			for (Number160 basedOnKey : versionTree.get(version)) {
				Number640 bKey = new Number640(version.locationKey(),
						version.domainKey(), version.contentKey(), basedOnKey);
				if (latestVersions.containsKey(bKey)) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * Joins the returned digest from all peers into a single map. Map will have
	 * no double entries.
	 * 
	 * @param rawDigest
	 *            digest result from all requested peers
	 * @return a version map
	 */
	public static NavigableMap<Number640, Set<Number160>> buildVersionTree(
			Map<PeerAddress, DigestResult> rawDigest) {
		NavigableMap<Number640, Set<Number160>> versionTree = new TreeMap<Number640, Set<Number160>>();
		if (rawDigest != null) {
			for (PeerAddress peerAddress : rawDigest.keySet()) {
				for (Number640 key : rawDigest.get(peerAddress).keyDigest()
						.keySet()) {
					for (Number160 bKey : rawDigest.get(peerAddress)
							.keyDigest().get(key)) {
						if (!versionTree.containsKey(key)) {
							versionTree.put(key, new HashSet<Number160>());
						}
						versionTree.get(key).add(bKey);
					}
				}
			}
		}
		return versionTree;
	}

	/**
	 * Joins all latest versions in one map.
	 * 
	 * @param peerDataMap
	 *            get result from all contacted peers
	 * @return map containing all latest versions
	 */
	public static NavigableMap<Number640, Data> buildVersions(
			Map<PeerAddress, Map<Number640, Data>> peerDataMap) {
		NavigableMap<Number640, Data> latestVersions = new TreeMap<Number640, Data>();
		if (peerDataMap != null) {
			for (PeerAddress peerAddress : peerDataMap.keySet()) {
				Map<Number640, Data> dataMap = peerDataMap.get(peerAddress);
				if (dataMap == null) {
					// ignore this peer
					// logger.warn("Received null. responder = '{}'",
					// peerAddress);
				} else if (dataMap.isEmpty()) {
					// ignore this peer
					// logger.warn("Received empty map. responder = '{}'",
					// peerAddress);
				} else {
					NavigableMap<Number640, Data> sortedDataMap = new TreeMap<Number640, Data>(
							dataMap);
					for (Number640 key : sortedDataMap.keySet()) {
						latestVersions.put(key, sortedDataMap.get(key));
					}
				}
			}
		}
		return latestVersions;
	}

	private static Random random = new Random();

	public static void waitAMoment() {
		while (true) {
			try {
				Thread.sleep(500 + random.nextInt(1000));
				break;
			} catch (InterruptedException e) {
				logger.error("Got interupted.", e);
			}
		}
	}

	public static String getVersionKeysFromPeers(
			Map<PeerAddress, Map<Number640, Data>> peerResult) {
		String result = "";
		for (PeerAddress peerAddress : peerResult.keySet()) {
			Map<Number640, Data> dataMap = peerResult.get(peerAddress);
			result += getVersionKeysFromMap(dataMap) + " ";
		}
		return result;
	}

	public static String getVersionKeysFromMap(Map<Number640, Data> dataMap) {
		String result = "";
		for (Number640 key : dataMap.keySet()) {
			result += key.versionKey() + " ";
		}
		return result;
	}

	public static String getVersionNumbersFromMap(Map<Number640, Data> dataMap) {
		String result = "";
		for (Number640 key : dataMap.keySet()) {
			result += key.versionKey().timestamp() + " ";
		}
		return result;
	}

	public static String getVersionKeysFromDigest(
			Map<PeerAddress, DigestResult> rawDigest) {
		String tmp = "";
		for (PeerAddress peerAddress : rawDigest.keySet()) {
			DigestResult digestResult = rawDigest.get(peerAddress);
			tmp += getVersionKeysFromMap(digestResult.keyDigest()) + " ";
		}
		return tmp;
	}

	public static String getVersionKeysFromMap(
			NavigableMap<Number640, Collection<Number160>> dataMap) {
		String tmp = "";
		for (Number640 key : dataMap.keySet()) {
			tmp += key.versionKey() + " ";
		}
		return tmp;
	}

	public static String getVersionKeysFromMap2(
			NavigableMap<Number640, Set<Number160>> versionTree) {
		String tmp = "";
		for (Number640 key : versionTree.keySet()) {
			tmp += key.versionKey() + " " + versionTree.get(key).toString()
					+ " ";
		}
		return tmp;
	}

	public static String getVersionNumbersFromMap2(
			NavigableMap<Number640, Set<Number160>> versionTree) {
		String tmp = "";
		for (Number640 key : versionTree.keySet()) {
			tmp += key.versionKey().timestamp() + "[";
			boolean first = true;
			for (Number160 bKey : versionTree.get(key)) {
				tmp += (first ? "" : " ") + bKey.timestamp();
				first = false;
			}
			tmp += "], ";
		}
		return tmp;
	}

	public static void removeOutdatedVersions(NavigableMap<Number640, ?> versions, int limit) {
		if (!versions.isEmpty()) {
			while (versions.firstKey().versionKey().timestamp()
					+ limit <= versions.lastKey()
					.versionKey().timestamp()) {
				versions.pollFirstEntry();
			}
		}
	}

}
