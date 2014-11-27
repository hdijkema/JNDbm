package net.dijkema.jndbm.datastruct;

public class Blob {
	String _key;
	byte[] _data;
	int    _dataSize;
	
	public byte[] getData() {
		return _data;
	}
	
	public int getDataSize() {
		return _dataSize;
	}
	
	public String key() {
		return _key;
	}
	
	public Blob(String key,byte[] data,int size) {
		_key=key;
		_data=data;
		_dataSize=size;
	}
	
	public Blob() {
	}
}
