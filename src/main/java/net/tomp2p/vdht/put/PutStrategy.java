package net.tomp2p.vdht.put;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number480;

/**
 * Abstraction for different putting approaches.
 * 
 * @author Seppi
 */
public abstract class PutStrategy {

	protected final String id;
	protected final Number480 key;

	private final Result result;

	public PutStrategy(String id, Number480 key, Result result) {
		this.id = id;
		this.key = key;
		this.result = result;
	}

	public abstract void getUpdateAndPut(PeerDHT peer) throws Exception;

	public void increaseWriteCounter() {
		result.increaseWriteCounter(id);
	}

	public void increaseMergeCounter() {
		result.increaseMergeCounter(id);
	}

	public void increaseDelayCounter() {
		result.increaseDelayCounter(id);
	}

	public void increaseForkAfterGetCounter() {
		result.increaseForkAfterGetCounter(id);
	}

	public void increaseForkAfterPutCounter() {
		result.increaseForkAfterPutCounter(id);
	}

	public void decreaseWriteCounter() {
		result.decreaseWriteCounter(id);
	}

	public void decreaseMergeCounter() {
		result.decreaseMergeCounter(id);
	}

	public void increaseConsistencyBreak() {
		result.increaseConsistencyBreak(id);
	}

	public int getWriteCounter() {
		return result.getWriteCounter(id);
	}

	public int getMergeCounter() {
		return result.getMergeCounter(id);
	}

}
