package net.tomp2p.vdht.put;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class Result {

	private final Data data;
	private final Number160 versionKey;

	public Result(Data data, Number160 versionKey) {
		this.data = data;
		this.versionKey = versionKey;
	}

	public Data getData() {
		return data;
	}

	public Number160 getVersionKey() {
		return versionKey;
	}

}