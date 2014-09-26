package net.tomp2p.vdht.put;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.peers.Number480;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods to protocol version writes, merges, delays, forks,
 * consistency breaks and runtimes.
 * 
 * @author Seppi
 */
public class Result {

	private final Logger logger = LoggerFactory.getLogger(Result.class);

	private final Map<String, Integer> writes = new HashMap<String, Integer>();
	private final Map<String, Integer> merges = new HashMap<String, Integer>();
	private final Map<String, Integer> delays = new HashMap<String, Integer>();
	private final Map<String, Integer> forksAfterPut = new HashMap<String, Integer>();
	private final Map<String, Integer> forksAfterGet = new HashMap<String, Integer>();
	private final Map<String, Integer> consistencyBreaks = new HashMap<String, Integer>();
	private final Map<String, Long> runtimes = new HashMap<String, Long>();

	private final Number480 key;

	private Map<String, Integer> latestVersion;

	public Result(Number480 key) {
		this.key = key;
	}

	public Number480 getKey() {
		return key;
	}

	public void setLatestVersion(Map<String, Integer> latestVersion) {
		this.latestVersion = latestVersion;
	}

	public synchronized void increaseWriteCounter(String id) {
		if (writes.containsKey(id)) {
			writes.put(id, writes.get(id) + 1);
		} else {
			writes.put(id, 1);
		}
	}

	public synchronized void decreaseWriteCounter(String id) {
		if (writes.containsKey(id)) {
			writes.put(id, writes.get(id) - 1);
		} else {
			logger.error("Should be never called.");
		}
	}

	public synchronized void decreaseMergeCounter(String id) {
		if (merges.containsKey(id)) {
			merges.put(id, merges.get(id) - 1);
		} else {
			logger.error("Should be never called.");
		}
	}

	public synchronized void increaseMergeCounter(String id) {
		if (merges.containsKey(id)) {
			merges.put(id, merges.get(id) + 1);
		} else {
			merges.put(id, 1);
		}
	}

	public synchronized void increaseDelayCounter(String id) {
		if (delays.containsKey(id)) {
			delays.put(id, delays.get(id) + 1);
		} else {
			delays.put(id, 1);
		}
	}

	public synchronized void increaseForkAfterGetCounter(String id) {
		if (forksAfterGet.containsKey(id)) {
			forksAfterGet.put(id, forksAfterGet.get(id) + 1);
		} else {
			forksAfterGet.put(id, 1);
		}
	}

	public synchronized void increaseForkAfterPutCounter(String id) {
		if (forksAfterPut.containsKey(id)) {
			forksAfterPut.put(id, forksAfterPut.get(id) + 1);
		} else {
			forksAfterPut.put(id, 1);
		}
	}

	public void increaseConsistencyBreak(String id) {
		if (consistencyBreaks.containsKey(id)) {
			consistencyBreaks.put(id, consistencyBreaks.get(id) + 1);
		} else {
			consistencyBreaks.put(id, 1);
		}
	}

	public synchronized void storeRuntime(String id, long runtime) {
		runtimes.put(id, runtime);
	}

	public int getWriteCounter(String id) {
		if (writes.containsKey(id)) {
			return writes.get(id);
		} else {
			return 0;
		}
	}

	public int getMergeCounter(String id) {
		if (merges.containsKey(id)) {
			return merges.get(id);
		} else {
			return 0;
		}
	}

	public int countWrites() {
		int counter = 0;
		for (int writes : writes.values()) {
			counter += writes;
		}
		return counter;
	}

	public int countMerges() {
		int counter = 0;
		for (int merges : merges.values()) {
			counter += merges;
		}
		return counter;
	}

	public int countDelays() {
		int counter = 0;
		for (int delays : delays.values()) {
			counter += delays;
		}
		return counter;
	}

	public int countForksAfterGet() {
		int counter = 0;
		for (int forksAfterGet : forksAfterGet.values()) {
			counter += forksAfterGet;
		}
		return counter;
	}

	public int countForksAfterPut() {
		int counter = 0;
		for (int forksAfterPut : forksAfterPut.values()) {
			counter += forksAfterPut;
		}
		return counter;
	}

	public int countVersions() {
		int counter = 0;
		if (latestVersion != null) {
			for (int versions : latestVersion.values()) {
				counter += versions;
			}
		}
		return counter;
	}

	public int countConsistencyBreaks() {
		int counter = 0;
		for (int consistencyBreaks : consistencyBreaks.values()) {
			counter += consistencyBreaks;
		}
		return counter;
	}

	public long getLongestRuntime() {
		return Collections.max(new ArrayList<Long>(runtimes.values()));
	}

	public void printResults() {
		logger.debug("latest version     = '{}'", latestVersion);
		logger.debug("version writes     = '{}'", writes);
		logger.debug("merges             = '{}'", merges);
		logger.debug("delays             = '{}'", delays);
		logger.debug("forks after get    = '{}'", forksAfterGet);
		logger.debug("forks after put    = '{}'", forksAfterPut);
		logger.debug("consistency breaks = '{}'", consistencyBreaks);
		logger.debug("runtimes           = '{}'", runtimes);
		// logger.debug("key             = '{}'", key);
	}
}
