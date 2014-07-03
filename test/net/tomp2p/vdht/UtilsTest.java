package net.tomp2p.vdht;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

	@Test
	public void testHasVersionDelay1() throws Exception {
		Map<Number640, Data> latestVersions = new HashMap<Number640, Data>();
		Map<Number640, Set<Number160>> versionTree = new HashMap<Number640, Set<Number160>>();

		Assert.assertFalse(Utils.hasVersionDelay(latestVersions, versionTree));
	}

	@Test
	public void testHasVersionDelay2() throws Exception {
		Map<Number640, Data> latestVersions = new HashMap<Number640, Data>();
		Map<Number640, Set<Number160>> versionTree = new HashMap<Number640, Set<Number160>>();

		Number480 key = new Number480(new Random());

		Number640 key0 = new Number640(key, Utils.generateVersionKey(Number160.ZERO, UUID.randomUUID()
				.toString()));
		Number640 key1 = new Number640(key, Utils.generateVersionKey(key0.versionKey(), UUID.randomUUID()
				.toString()));
		Number640 key2 = new Number640(key, Utils.generateVersionKey(key1.versionKey(), UUID.randomUUID()
				.toString()));

		latestVersions.put(key2, null);

		HashSet<Number160> set0 = new HashSet<Number160>();
		set0.add(Number160.ZERO);
		HashSet<Number160> set1 = new HashSet<Number160>();
		set1.add(key0.versionKey());
		HashSet<Number160> set2 = new HashSet<Number160>();
		set2.add(key1.versionKey());

		versionTree.put(key0, set0);
		versionTree.put(key1, set1);
		versionTree.put(key2, set2);

		Assert.assertFalse(Utils.hasVersionDelay(latestVersions, versionTree));
	}

	@Test
	public void testHasVersionDelay3() throws Exception {
		Map<Number640, Data> latestVersions = new HashMap<Number640, Data>();
		Map<Number640, Set<Number160>> versionTree = new HashMap<Number640, Set<Number160>>();

		Number480 key = new Number480(new Random());

		Number640 key0 = new Number640(key, Utils.generateVersionKey(Number160.ZERO, UUID.randomUUID()
				.toString()));
		Number640 key1 = new Number640(key, Utils.generateVersionKey(key0.versionKey(), UUID.randomUUID()
				.toString()));
		Number640 key2 = new Number640(key, Utils.generateVersionKey(key1.versionKey(), UUID.randomUUID()
				.toString()));

		latestVersions.put(key0, null);

		HashSet<Number160> set0 = new HashSet<Number160>();
		set0.add(Number160.ZERO);
		HashSet<Number160> set1 = new HashSet<Number160>();
		set1.add(key0.versionKey());
		HashSet<Number160> set2 = new HashSet<Number160>();
		set2.add(key1.versionKey());

		versionTree.put(key0, set0);
		versionTree.put(key1, set1);
		versionTree.put(key2, set2);

		Assert.assertTrue(Utils.hasVersionDelay(latestVersions, versionTree));

		latestVersions.put(key1, null);

		Assert.assertTrue(Utils.hasVersionDelay(latestVersions, versionTree));

		latestVersions.put(key2, null);

		Assert.assertTrue(Utils.hasVersionDelay(latestVersions, versionTree));
	}

}
