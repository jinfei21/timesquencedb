package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListSet;
import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IFileIterator;
import com.ctriposs.tsdb.common.Level;
import com.ctriposs.tsdb.common.PureFileStorage;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;

public class LevelSeekIterator implements ISeekIterator<InternalKey, byte[]> {

	private FileManager fileManager;
	private ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>> itSet;
	private Direction direction;
	private Entry<InternalKey, byte[]> curEntry;
	private IFileIterator<InternalKey, byte[]> curIt;
	private long curSeekTime;
	private InternalKey seekKey;
	private Level level;
	private long interval;

	public LevelSeekIterator(FileManager fileManager, Level level, long interval) {
		this.fileManager = fileManager;
		this.level = level;
		this.interval = interval;
		this.direction = Direction.forward;
		this.curEntry = null;
		this.curIt = null;
		this.itSet = null;
		this.curSeekTime = 0;
	}

	@Override
	public boolean hasNext() {

		boolean result = false;
		if (itSet != null) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				if (it.hasNext()) {
					result = true;
					break;
				}
			}
		}

		if (!result) {
			curSeekTime += interval;
			if (curSeekTime < System.currentTimeMillis()) {
				try {
					itSet = getNextIterators(curSeekTime);
					if (null != itSet) {
						for (IFileIterator<InternalKey, byte[]> it : itSet) {
							it.seek(seekKey.getCode(), curSeekTime);
						}
						findSmallest();
						direction = Direction.forward;
					}
				} catch (IOException e) {
					result = false;
					throw new RuntimeException(e);
				}
			} else {
				result = false;
			}
		}

		return result;
	}

	@Override
	public boolean hasPrev() {
		boolean result = false;
		if (itSet != null) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				if (it.hasPrev()) {
					result = true;
					break;
				}
			}
		}

		if (!result) {
			curSeekTime -= interval;
			if (curSeekTime > System.currentTimeMillis()
					- fileManager.getMaxPeriod()) {
				try {
					itSet = getPrevIterators(curSeekTime);
					if (null != itSet) {
						for (IFileIterator<InternalKey, byte[]> it : itSet) {
							it.seek(seekKey.getCode(), curSeekTime);
						}
						findLargest();
						direction = Direction.reverse;
					}
				} catch (IOException e) {
					result = false;
					throw new RuntimeException(e);
				}
			} else {
				result = false;
			}
		}

		return result;
	}

	@Override
	public Entry<InternalKey, byte[]> next() {
		if (direction != Direction.forward) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {

				if (it != curIt) {
					try {
						if (it.hasNext()) {
							it.seek(seekKey.getCode(), curSeekTime);
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			direction = Direction.forward;
		}
		curEntry = curIt.next();
		findSmallest();
		return curEntry;
	}

	@Override
	public Entry<InternalKey, byte[]> prev() {
		if (direction != Direction.reverse) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				if (curIt != it) {
					try {
						if (it.hasNext()) {
							it.seek(seekKey.getCode(), curSeekTime);
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			direction = Direction.reverse;
		}
		curEntry = curIt.prev();
		findLargest();
		return curEntry;
	}

	@Override
	public void seek(String table, String column, long time) throws IOException {

		seekKey = new InternalKey(fileManager.getCode(table),
				fileManager.getCode(column), time);

		itSet = getNextIterators(level.format(time));

		if (null != itSet) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				it.seek(seekKey.getCode(), curSeekTime);
			}
			findSmallest();
			direction = Direction.forward;
		}
	}

	private void findSmallest() {
		if (null != itSet) {
			IFileIterator<InternalKey, byte[]> smallest = null;

			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				if (it.valid()) {
					if (smallest == null) {
						smallest = it;
					} else if (fileManager.compare(smallest.key(), it.key()) > 0) {
						smallest = it;
					} else if (fileManager.compare(smallest.key(), it.key()) == 0) {
						while (it.hasNext()) {
							it.next();
							int diff = fileManager.compare(smallest.key(),
									it.key());
							if (0 == diff) {
								continue;
							} else {
								break;
							}
						}
					}
				}
			}
			curIt = smallest;
		}
	}

	private void findLargest() {
		if (null != itSet) {
			IFileIterator<InternalKey, byte[]> largest = null;
			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				if (it.valid()) {
					if (largest == null) {
						largest = it;
					} else if (fileManager.compare(largest.key(), it.key()) < 0) {
						largest = it;
					} else if (fileManager.compare(largest.key(), it.key()) == 0) {
						while (it.hasPrev()) {
							it.prev();
							int diff = fileManager.compare(largest.key(),it.key());
							if (0 == diff) {
								continue;
							} else {
								break;
							}
						}
					}
				}
			}
			curIt = largest;
		}
	}

	private ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>> getNextIterators(long time) throws IOException {

		if (time > System.currentTimeMillis()) {
			return null;
		}

		ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>> set = getIterators(time);
		if (set != null) {

			if (itSet != null) {
				for (IFileIterator<InternalKey, byte[]> it : itSet) {
					it.close();
				}
			}
			return set;
		} else {
			return getNextIterators(time + interval);
		}
	}

	private ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>> getIterators(long time) throws IOException {
		curSeekTime = time;
		ConcurrentSkipListSet<FileMeta> metaSet = level.getFiles(time);
		if (metaSet != null) {
			ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>> set = new ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>>();

			for (FileMeta meta : metaSet) {
				set.add(new FileSeekIterator(new PureFileStorage(meta.getFile()), meta.getFileNumber()));
			}
			return set;
		} else {
			return null;
		}
	}

	private ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>> getPrevIterators(
			long time) throws IOException {

		if (time < System.currentTimeMillis() - fileManager.getMaxPeriod()) {
			return null;
		}

		ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>> set = getIterators(time);
		if (set != null) {

			if (itSet != null) {
				for (IFileIterator<InternalKey, byte[]> it : itSet) {
					it.close();
				}
			}
			return set;
		} else {
			return getPrevIterators(time - interval);
		}
	}

	@Override
	public String table() {
		if (curEntry != null) {
			fileManager.getName(curEntry.getKey().getTableCode());
		}
		return null;
	}

	@Override
	public String column() {
		if (curEntry != null) {
			fileManager.getName(curEntry.getKey().getColumnCode());
		}
		return null;
	}

	@Override
	public long time() {
		if (curEntry != null) {
			return curEntry.getKey().getTime();
		}
		return 0;
	}

	@Override
	public byte[] value() throws IOException {
		if (curEntry != null) {
			return curEntry.getValue();
		}
		return null;
	}

	@Override
	public boolean valid() {
		if (curEntry == null) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public void close() throws IOException {

		if (null != itSet) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				it.close();
			}
		}
	}

	@Override
	public InternalKey key() {
		if (curEntry != null) {
			return curEntry.getKey();
		}
		return null;
	}

	public int getLevelNum() {
		return level.getLevelNum();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}

	enum Direction {
		forward, reverse
	}

}
