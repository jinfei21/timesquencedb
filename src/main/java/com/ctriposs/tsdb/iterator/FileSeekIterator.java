package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Map.Entry;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalEntry;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.NameManager;
import com.ctriposs.tsdb.storage.DataMeta;
import com.ctriposs.tsdb.util.ByteUtil;

public class FileSeekIterator implements
		IInternalSeekIterator<InternalKey, byte[]> {

	private final NameManager nameManager;
	private int curPos = -1;
	private IStorage storage;
	private int maxPos = 0;
	private DataMeta curMeta;
	private Entry<InternalKey, byte[]> curEntry;
	private long seekCode = -1L;

	public FileSeekIterator(IStorage storage, NameManager nameManager)
			throws IOException {
		this.storage = storage;
		byte[] bytes = new byte[4];
		this.storage.get(0, bytes);
		this.maxPos = ByteUtil.ToInt(bytes);
		this.curMeta = null;
		this.curEntry = null;
		this.nameManager = nameManager;
	}

	@Override
	public boolean hasNext() {
		if (curPos < maxPos) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Entry<InternalKey, byte[]> next() {
		if (curPos < maxPos) {
			curPos++;
			try {
				curMeta = read(curPos);
				if (curMeta.getCode() == seekCode||seekCode == -1) {

					InternalKey key = new InternalKey(curMeta.getCode(),
							curMeta.getTime());
					byte[] value = new byte[curMeta.getValueSize()];
					storage.get(curMeta.getOffSet(), value);
					curEntry = new InternalEntry(key, value);
				} else {
					curEntry = null;
					curPos = maxPos;
				}
			} catch (IOException e) {
				e.printStackTrace();
				curEntry = null;
			}

		} else {
			curEntry = null;
		}
		return curEntry;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}
	

	@Override
	public void seekToFirst() throws IOException {
		curPos = 0;
		if(curPos < maxPos){
			curMeta = read(curPos);
			InternalKey key = new InternalKey(curMeta.getCode(),
					curMeta.getTime());
			byte[] value = new byte[curMeta.getValueSize()];
			storage.get(curMeta.getOffSet(), value);
			curEntry = new InternalEntry(key, value);
		}
	}


	@Override
	public void seek(long code) throws IOException {

		int left = 0;
		int right = maxPos - 1;
		seekCode = code;
		while (left < right) {
			int mid = (left + right + 1) / 2;
			curMeta = read(mid);
			if (code < curMeta.getCode()) {
				right = mid - 1;
			} else if (code > curMeta.getCode()) {
				left = mid + 1;
			} else {
				curPos = mid;
				break;
			}
		}

		if (left < right) {
			for (int pos = curPos - 1; pos >= 0; pos--) {
				DataMeta meta = read(pos);
				if (meta.getCode() != code) {
					curPos = pos + 1;
					break;
				}
			}

			curMeta = read(curPos);
			InternalKey key = new InternalKey(curMeta.getCode(),
					curMeta.getTime());
			byte[] value = new byte[curMeta.getValueSize()];
			storage.get(curMeta.getOffSet(), value);
			curEntry = new InternalEntry(key, value);
		} else {
			curPos = maxPos + 1;
		}
	}

	private DataMeta read(int index) throws IOException {
		byte[] bytes = new byte[DataMeta.META_SIZE];
		storage.get(4 + DataMeta.META_SIZE * index, bytes);
		return new DataMeta(bytes);
	}

	@Override
	public String table() {
		if (curEntry != null) {
			nameManager.getName(curEntry.getKey().getTableCode());
		}
		return null;
	}

	@Override
	public String column() {
		if (curEntry != null) {
			nameManager.getName(curEntry.getKey().getColumnCode());
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
	public byte[] value() {
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
	public Entry<InternalKey, byte[]> prev() {
		if (curPos > 0) {
			curPos--;
			try {
				curMeta = read(curPos);
				if (curMeta.getCode() == seekCode||seekCode == -1) {
					InternalKey key = new InternalKey(curMeta.getCode(),
							curMeta.getTime());
					byte[] value = new byte[curMeta.getValueSize()];
					storage.get(curMeta.getOffSet(), value);
					curEntry = new InternalEntry(key, value);

				} else {
					curEntry = null;
					curPos = -1;
				}

			} catch (IOException e) {
				e.printStackTrace();
				curEntry = null;
			}

		} else {
			curEntry = null;
		}
		return curEntry;
	}

	@Override
	public void close() throws IOException {
		storage.close();
	}

	@Override
	public InternalKey key() {
		if (curEntry != null) {
			return curEntry.getKey();
		}
		return null;
	}

}
