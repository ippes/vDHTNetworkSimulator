package net.tomp2p.vdht;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import net.tomp2p.peers.Number160;

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

}
