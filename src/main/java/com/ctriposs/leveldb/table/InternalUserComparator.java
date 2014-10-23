package com.ctriposs.leveldb.table;

import com.ctriposs.leveldb.Constant;
import com.google.common.base.Preconditions;

public class InternalUserComparator implements UserComparator {
	private final InternalKeyComparator internalKeyComparator;

	public InternalUserComparator(InternalKeyComparator internalKeyComparator) {
		this.internalKeyComparator = internalKeyComparator;
	}

	@Override
	public int compare(Slice left, Slice right) {
		return internalKeyComparator.compare(new InternalKey(left),
				new InternalKey(right));
	}

	@Override
	public String name() {
		return internalKeyComparator.name();
	}

	@Override
	public Slice findShortSeparator(Slice start, Slice limit) {
		// Attempt to shorten the user portion of the key
		Slice startUserKey = new InternalKey(start).getUserKey();
		Slice limitUserKey = new InternalKey(limit).getUserKey();

		Slice shortestSeparator = internalKeyComparator.getUserComparator().findShortSeparator(startUserKey, limitUserKey);

		if (internalKeyComparator.getUserComparator().compare(startUserKey,shortestSeparator) < 0) {
			// User key has become larger. Tack on the earliest possible
			// number to the shortened user key.
			InternalKey newInternalKey = new InternalKey(shortestSeparator,Constant.MAX_SEQUENCE, ValueType.VALUE);
			Preconditions.checkState(compare(start, newInternalKey.encode()) < 0);// todo
			Preconditions.checkState(compare(newInternalKey.encode(), limit) < 0);// todo

			return newInternalKey.encode();
		}

		return start;
	}

	@Override
	public Slice findShortSuccessor(Slice key) {
		Slice userKey = new InternalKey(key).getUserKey();
		Slice shortSuccessor = internalKeyComparator.getUserComparator().findShortSuccessor(userKey);

		if (internalKeyComparator.getUserComparator().compare(userKey,shortSuccessor) < 0) {
			// User key has become larger. Tack on the earliest possible
			// number to the shortened user key.
			InternalKey newInternalKey = new InternalKey(shortSuccessor,Constant.MAX_SEQUENCE, ValueType.VALUE);
			Preconditions.checkState(compare(key, newInternalKey.encode()) < 0);// todo

			return newInternalKey.encode();
		}

		return key;
	}
}
