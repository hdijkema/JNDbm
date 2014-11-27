package net.dijkema.jndbm;

import java.awt.Color;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import net.dijkema.jndbm.datastruct.Types;
import net.dijkema.jndbm.streams.NDbmByteArrayOutputStream;
import net.dijkema.jndbm.streams.NDbmDataOutputStream;
import net.dijkema.jndbm.util.Base64;
import net.dijkema.jndbm.writers.NDbmEncWriter;

public class NDbmEncoder {

	
	
	private NDbmEncDec 					_encoder;
	private String    					_data;
	private NDbmByteArrayOutputStream 	_bout;
	private DataOutput              	_dout;
	private boolean						_closed;
	
	public void writeColor(Color c)							{ _encoder.writeColor(_dout,c); }
	public void writeInt(int i) 							{ _encoder.writeInt(_dout,i); }
	public void writeLong(long i) 							{ _encoder.writeLong(_dout,i); }
	public void writeString(String s) 						{ _encoder.writeString(_dout,s); }
	public void writeBoolean(boolean b)						{ _encoder.writeBoolean(_dout,b); }
	public void writeObject(NDbmObjectWriter w)				{ _encoder.writeObject(_dout,w); }
	public void writeDate(java.util.Date d)					{ _encoder.writeDate(_dout,d); }
	public void writeType(char t)							{ _encoder.writeType(_dout,t); }
	
	public <K, T> void writeHashtable(Hashtable<K,T> h,NDbmEncWriter<K> kw,NDbmEncWriter<T> vw) {
		if (h==null) {
			writeType(Types.TYPE_NULL_HASHTABLE);
		} else {
			writeType(Types.TYPE_HASHTABLE);
			writeInt(h.size());
			Enumeration<K> en=h.keys();
			while(en.hasMoreElements()) {
				K key=en.nextElement();
				kw.write(this,key);
				vw.write(this,h.get(key));
			}
		}
	}
	
	public <T> void writeVector(Vector<T> v,NDbmEncWriter<T> w) {
		if (v==null) {
			writeType(Types.TYPE_NULL_VECTOR); 
		} else {
			writeType(Types.TYPE_VECTOR);
			writeInt(v.size());
			Iterator<T> it=v.iterator();
			while(it.hasNext()) { w.write(this, it.next()); }
		}
	}
	
	public <T> void writeSet(Set<T> v,NDbmEncWriter<T> w) {
		if (v==null) {
			writeType(Types.TYPE_NULL_SET);
		} else {
			writeType(Types.TYPE_SET);
			writeInt(v.size());
			Iterator<T> it=v.iterator();
			while(it.hasNext()) { w.write(this, it.next()); }
		}
	}
	
	public <T> void writeArray(T[] a,NDbmEncWriter<T> tr) {
		if (a==null) {
			writeType(Types.TYPE_NULL_ARRAY);
		} else {
			writeType(Types.TYPE_ARRAY);
			writeInt(a.length);
			int i,N=a.length;
			for(i=0;i<N;i++) {
				tr.write(this,a[i]);
			}
		}
	}
	
	public void close() throws Exception {
		_closed=true;
	}
	
	public String toString()  {
		if (_closed) {
			//if (_data==null) {
			//	_data=Base64.encodeToString(_bout.bytes(), true);
			//}
			try {
				_data=new String(_bout.toString("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}  // TODO: UTF-8  handling?
			return _data;
		} else {
			return null;
		}
	}
	
	public byte[] toBytes() {
		return _bout.bytes();
	}

	public NDbmEncoder() throws Exception {
		_encoder=new NDbmEncDec();
		_bout=new NDbmByteArrayOutputStream();
		_dout=new NDbmDataOutputStream(_bout);
		_data=null;
		_closed=false;
	}
}
