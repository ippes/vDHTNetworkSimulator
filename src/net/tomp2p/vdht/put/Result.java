package net.tomp2p.vdht.put;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.peers.Number480;

public class Result {

	private final Logger logger = LoggerFactory.getLogger(Result.class);

	private final Map<String, Integer> versions = new HashMap<String, Integer>();

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

	public void putWriteCounter(String id, int writes) {
		versions.put(id, writes);
	}

	public int countWrites() {
		int counter = 0;
		for (int writes : versions.values()) {
			counter += writes;
		}
		return counter;
	}

	public int countVersions() {
		int counter = 0;
		for (int versions : latestVersion.values()) {
			counter += versions;
		}
		return counter;
	}

	public void printResults() {
		logger.debug("latest version = '{}' version writes = '{}' key = '{}'", latestVersion, versions, key);
	}
}
