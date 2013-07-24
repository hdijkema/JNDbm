package net.oesterholt.jndbm;

import java.awt.Color;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import net.oesterholt.jndbm.datastruct.Types;
import net.oesterholt.jndbm.readers.NDbmDecReader;
import net.oesterholt.jndbm.streams.NDbmByteArrayInputStream;
import net.oesterholt.jndbm.streams.NDbmDataInputStream;

import com.sun.org.apache.xml.internal.security.utils.Base64;


public class NDbmDecoder {
	private NDbmEncDec 					_decoder;
	private String    					_data;
	private NDbmByteArrayInputStream 	_bin;
	private NDbmDataInputStream     	_din;
	private boolean						_closed;
		
	public Color   readColor()								{ return _decoder.readColor(_din); }
	public int     readInt() 								{ return _decoder.readInt(_din); }
	public long    readLong() 								{ return _decoder.readLong(_din); }
	public String  readString() 							{ return _decoder.readString(_din); }
	public boolean readBoolean()							{ return _decoder.readBoolean(_din); }
	public void    readObject(NDbmObjectReader r)			{ _decoder.readObject(_din,r); }
	public java.util.Date readDate()					    { return _decoder.readDate(_din); }
	public char    readType(char t)							{ return _decoder.readType(_din); }
		
	public <K, T> Hashtable<K,T> readHashtable(NDbmDecReader<K> kr,NDbmDecReader<T> tr) {
		char t=_decoder.checkType(_din,Types.TYPE_HASHTABLE,Types.TYPE_NULL_HASHTABLE);
		if (t==Types.TYPE_NULL_HASHTABLE) {
			return null;
		} else {
			Hashtable<K,T> h=new Hashtable<K,T>();
			int size=readInt();
			int i;
			for(i=0;i<size;i++) {
				K key=kr.read(this);
				T v=tr.read(this);
				h.put(key, v);
			}
			return h;
		}
	}
	
	private <T> Vector<T> readInternalVector(char tt,char nt, NDbmDecReader<T> tr) {
		char t=_decoder.checkType(_din,tt,nt);
		if (t==nt) {
			return null;
		} else {
			int size=readInt();
			int i;
			Vector<T> v=new Vector<T>();
			for(i=0;i<size;i++) {
				T val=tr.read(this);
				v.add(val);
			}
			return v;
		}
	}
	
	public <T> Vector<T> readVector(NDbmDecReader<T> tr) {
		return readInternalVector(Types.TYPE_VECTOR,Types.TYPE_NULL_VECTOR,tr);
	}
	
	public <T> Set<T> readSet(NDbmDecReader<T> tr) {
		Vector<T> vt=readInternalVector(Types.TYPE_SET,Types.TYPE_NULL_SET,tr);
		if (vt==null) {
			return null;
		} else {
			HashSet<T> hs=new HashSet<T>(vt);
			return hs;
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T> T[] readArray(NDbmDecReader<T> tr) {
		Vector<T> t=readInternalVector(Types.TYPE_ARRAY,Types.TYPE_NULL_ARRAY,tr);
		if (t!=null) {
			//T[] g=new T[t.size()];
			return (T[]) t.toArray();
		} else {
			return null;
		}
	}
	
	public <T> Vector<T> readArrayAsVector(NDbmDecReader<T> tr) {
		return readInternalVector(Types.TYPE_ARRAY,Types.TYPE_NULL_ARRAY,tr);
	}
	
	public boolean closed() {
		return _closed;
	}
	
	public void close() throws Exception {
		_closed=true;
	}
		
	public String toString() {
		return _data;
	}

	public NDbmDecoder(String data) throws Exception {
		_decoder=new NDbmEncDec();
		_data=data;
		//_bin=new NDbmByteArrayInputStream(Base64.decode(_data));
		_bin=new NDbmByteArrayInputStream(_data.getBytes("UTF-8"));
		_din=new NDbmDataInputStream(_bin);
		_closed=false;
	}
}
