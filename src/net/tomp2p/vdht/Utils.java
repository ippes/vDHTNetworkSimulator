package net.tomp2p.vdht;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.tomp2p.dht.StorageLayer.PutStatus;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

public class Utils {

	public static Number160 generateVersionKey() {
		// get time stamp
		long timestamp = System.currentTimeMillis();
		// create new version key based on time stamp
		return new Number160(timestamp);
	}

	public static Number160 generateVersionKey(Serializable object) throws IOException {
		// get time stamp
		long timestamp = System.currentTimeMillis();
		// get a MD5 hash of the object itself
		byte[] hash = generateMD5Hash(serializeObject(object));
		// create new version key based on time stamp and hash
		return new Number160(timestamp, new Number160(Arrays.copyOf(hash, Number160.BYTE_ARRAY_SIZE)));
	}

	private static byte[] generateMD5Hash(byte[] data) {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
		}
		md.reset();
		md.update(data, 0, data.length);
		return md.digest();
	}

	private static byte[] serializeObject(Serializable object) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		byte[] result = null;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			result = baos.toByteArray();
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (oos != null)
					oos.close();
				if (baos != null)
					baos.close();
			} catch (IOException e) {
				throw e;
			}
		}
		return result;
	}

	public static boolean hasVersionFork(Map<PeerAddress, Map<Number640, Byte>> peerResult)
			throws IllegalStateException {
		// check result of all contacted peers
		for (PeerAddress peerAddress : peerResult.keySet()) {
			Map<Number640, Byte> putResult = peerResult.get(peerAddress);
			if (putResult.size() != 1) {
				throw new IllegalStateException(String.format(
						"Received wrong sized data map. peerAddress = '%s' size = '%s'", peerAddress,
						putResult.size()));
			} else {
				Entry<Number640, Byte> result = new TreeMap<Number640, Byte>(putResult).firstEntry();
				if (result.getValue().intValue() == PutStatus.OK.ordinal()) {
					continue;
				} else if (result.getValue().intValue() == PutStatus.VERSION_FORK.ordinal()) {
					// contains a version fork
					return true;
				} else {
					throw new IllegalStateException(String.format(
							"Received not handled put status as result. peerAddress = '%s' putStatus = '%s'",
							peerAddress, PutStatus.values()[result.getValue().intValue()]));
				}
			}
		}
		return false;
	}
}
