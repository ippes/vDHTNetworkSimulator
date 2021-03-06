package net.tomp2p.vdht;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * source: http://stackoverflow.com/questions/5639870/simple-java-name-based-locks
 */
public class KeyLock<K> {

	private static Logger logger = LoggerFactory.getLogger(KeyLock.class);

	public class RefCounterLock {
		final private K key;
		final public ReentrantLock lock = new ReentrantLock();

		private int counter = 0;

		public RefCounterLock(K key) {
			this.key = key;
		}
	}

	private final ReentrantLock lockInternal = new ReentrantLock();
	private final HashMap<K, RefCounterLock> cache = new HashMap<K, RefCounterLock>();

	public RefCounterLock tryLock(final K key) {
		final RefCounterLock refLock;
		lockInternal.lock();
		try {
			if (!cache.containsKey(key)) {
				refLock = new RefCounterLock(key);
				cache.put(key, refLock);
			} else {
				refLock = cache.get(key);
			}
		} finally {
			lockInternal.unlock();
		}
		if (refLock.lock.tryLock()) {
			refLock.counter++;
			return refLock;
		} else {
			return null;
		}
	}

	public void unlock(final RefCounterLock lock) throws IllegalMonitorStateException {
		RefCounterLock cachedLock = null;
		lockInternal.lock();
		try {
			if (cache.containsKey(lock.key)) {
				cachedLock = cache.get(lock.key);
				if (lock != cachedLock) {
					throw new IllegalArgumentException("Lock does not matches the stored lock.");
				}
				cachedLock.counter--;
				
				cachedLock.lock.unlock();
				// check if last reference
				if (cachedLock.counter == 0) {
					cache.remove(lock.key);
				}
			} else {
				logger.warn("Received an unknown lock.");;
			}
		} finally {
			lockInternal.unlock();
		}
	}

}