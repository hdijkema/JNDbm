package net.dijkema.jndbm;

import java.io.File;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import net.dijkema.jndbm.datastruct.Blob;
import net.dijkema.jndbm.streams.NDbmByteArrayInputStream;
import net.dijkema.jndbm.streams.NDbmByteArrayOutputStream;
import net.dijkema.jndbm.streams.NDbmDataInputStream;
import net.dijkema.jndbm.streams.NDbmDataOutputStream;
import net.dijkema.jndbm2.exceptions.NDbmException;

/*
 * This class is meant to wrap NDbm2 to gain performance. USE THIS CLASS ONLY IF YOU
 * KNOW WHAT YOU'RE DOING. This class eliminates the locking mechanisms of NDbm2.
 * 
 * This class doesn't implement the Object related methods of NDbm2. 
 * 
 */
public class NDbm2Caching {

    //////////////////////////////////////////////////////////////////////////////////////////////////
	
	interface NC {
		public void put(NDbm2 db) throws NDbmException;
		public void get(NDbm2 db) throws NDbmException;
		public boolean remover();
		public boolean flushed();
	};

	class NC_String implements NC {
		private String key;
		private String data;
		private boolean f;
		public NC_String(String k,String d) { key=k;data=d;f=false; }
		public NC_String(String k) { key=k;data=null;f=false; }
		public void put(NDbm2 db) throws NDbmException { db.putStr(key, data);f=true; }
		public void get(NDbm2 db) throws NDbmException { data=db.getStr(key);f=true; }
		public String val() { return data; }
		public boolean remover() { return false; }
		public boolean flushed() { return f; }
	}

	class NC_VectorOfString implements NC {
		private String key;
		private Vector<String> data;
		private boolean f;
		public NC_VectorOfString(String k,Vector<String> d) { key=k;data=d;f=false; }
		public NC_VectorOfString(String k) { key=k;data=null;f=false; }
		public void put(NDbm2 db) throws NDbmException { db.putVectorOfString(key, data);f=true; }
		public void get(NDbm2 db) throws NDbmException { data=db.getVectorOfString(key);f=true; }
		public Vector<String> val() { return data; }
		public boolean remover() { return false; }	
		public boolean flushed() { return f; }
	}
	
	class NC_Boolean implements NC {
		private String key;
		private Boolean data;
		private boolean f;
		public NC_Boolean(String k,Boolean d) { key=k;data=d;f=false; }
		public NC_Boolean(String k) { key=k;data=null;f=false; }
		public void put(NDbm2 db) throws NDbmException { db.putBoolean(key, data);f=true; }
		public void get(NDbm2 db) throws NDbmException { data=db.getBoolean(key);f=true; }
		public Boolean val() { return data; }
		public boolean remover() { return false; }	
		public boolean flushed() { return f; }
	}
	
	class NC_Integer implements NC {
		private String  key;
		private Integer data;
		private boolean f;
		public NC_Integer(String k,int d) { key=k;data=d;f=false; }
		public NC_Integer(String k) { key=k;data=null;f=false; }
		public void put(NDbm2 db) throws NDbmException { db.putInt(key, data);f=true; }
		public void get(NDbm2 db) throws NDbmException { data=db.getInt(key);f=true; }
		public Integer val() { return data; }
		public boolean remover() { return false; }	
		public boolean flushed() { return f; }
	}

	class NC_Long implements NC {
		private String  key;
		private Long    data;
		private boolean f;
		public NC_Long(String k,long d) { key=k;data=d;f=false; }
		public NC_Long(String k) { key=k;data=null;f=false; }
		public void put(NDbm2 db) throws NDbmException { db.putLong(key, data);f=true; }
		public void get(NDbm2 db) throws NDbmException { data=db.getLong(key);f=true; }
		public Long val() { return data; } 
		public boolean remover() { return false; }	
		public boolean flushed() { return f; }
	}

	class NC_Blob implements NC {
		private String  key;
		private Blob    data;
		private boolean f;
		public NC_Blob(String k,Blob d) { key=k;data=d;f=false; }
		public NC_Blob(String k) { key=k;data=null;f=false; }
		public void put(NDbm2 db) throws NDbmException { db.putBlob(key, data);f=true; }
		public void get(NDbm2 db) throws NDbmException { data=db.getBlobData(key);f=true; }
		public Blob val() { return data; }
		public boolean remover() { return false; }	
		public boolean flushed() { return f; }
	}
	
	
	class NC_Remove implements NC {
		private String key;
		private boolean f;
		public NC_Remove(String k) { key=k;f=false; }
		public void put(NDbm2 db) throws NDbmException { db.remove(key);f=true; }
		public void get(NDbm2 db) {}
		public boolean remover() { return true; }	
		public boolean flushed() { return f; }
	}
	
	////////////////////////////////////////////////////////////////////////////////////////////////////
	
	private NDbm2					_dbm=null;
	private Hashtable<String,NC>	_hash;
	private boolean                 _caching;
	
	public NDbm2Caching(File base,boolean readonly) throws NDbmException {
		_dbm=NDbm2.openNDbm(base,readonly);
		_hash=new Hashtable<String,NC>();
		_caching=true;
	}
	
	public NDbm2Caching(NDbm2 db) {
		_dbm=db;
		_hash=new Hashtable<String,NC>();
		_caching=true;
	}
	
	public void setCaching(boolean b) throws NDbmException {
		_caching=b;
		if (!_caching) {
			clearCache();
		}
	}
	
	public synchronized void putStr(String key,String val) throws NDbmException {
		if (_caching) {
			_hash.put(key, new NC_String(key,val));
		} else {
			_dbm.putStr(key, val);
		}
	}
	
	public synchronized String getStr(String key) throws NDbmException {
		if (_caching) {
			NC d=_hash.get(key);
			if (d==null) {
				NC_String dd=new NC_String(key);
				dd.get(_dbm);
				_hash.put(key, dd);
				return dd.val();
			} else {
				if (d.remover()) {
					return null;
				} else {
					return ((NC_String) d).val();
				}
			}
		} else {
			return _dbm.getStr(key);
		}
	}
	
	public synchronized void putBoolean(String key,Boolean b) throws NDbmException {
		if (_caching) {
			_hash.put(key, new NC_Boolean(key,b));
		} else {
			_dbm.putBoolean(key, b);
		}
	}
	
	public synchronized Boolean getBoolean(String key) throws NDbmException {
		if (_caching) {
			NC d=_hash.get(key);
			if (d==null) {
				NC_Boolean dd=new NC_Boolean(key);
				dd.get(_dbm);
				_hash.put(key, dd);
				return dd.val();
			} else {
				if (d.remover()) {
					return null;
				} else {
					return ((NC_Boolean) d).val();
				}
			}
		} else {
			return _dbm.getBoolean(key);
		}
	}
	
	public synchronized void putInt(String key,int b) throws NDbmException {
		if (_caching) {
			_hash.put(key, new NC_Integer(key,b));
		} else {
			_dbm.putInt(key, b);
		}
	}
	
	public synchronized Integer getInt(String key) throws NDbmException {
		if (_caching) {
			NC d=_hash.get(key);
			if (d==null) {
				NC_Integer dd=new NC_Integer(key);
				dd.get(_dbm);
				_hash.put(key, dd);
				return dd.val();
			} else {
				if (d.remover()) {
					return null;
				} else {
					return ((NC_Integer) d).val();
				}
			}
		} else {
			return _dbm.getInt(key);
		}
	}
	
	public synchronized void putVectorOfString(String key, Vector<String> data) throws NDbmException {
		if (_caching) {
			_hash.put(key, new NC_VectorOfString(key,data));
		} else {
			_dbm.putVectorOfString(key, data);
		}
	}
	
	public synchronized Vector<String> getVectorOfString(String key) throws NDbmException {
		if (_caching) {
			NC d=_hash.get(key);
			if (d==null) {
				NC_VectorOfString dd=new NC_VectorOfString(key);
				dd.get(_dbm);
				_hash.put(key, dd);
				return dd.val();
			} else {
				if (d.remover()) {
					return null;
				} else {
					return ((NC_VectorOfString) d).val();
				}
			}
		} else {
			return _dbm.getVectorOfString(key);
		}
	}
	
	public synchronized void putLong(String key,long b) throws NDbmException {
		if (_caching) {
			_hash.put(key, new NC_Long(key,b));
		} else {
			_dbm.putLong(key, b);
		}
	}
	
	public synchronized Long getLong(String key) throws NDbmException {
		if (_caching) {
			NC d=_hash.get(key);
			if (d==null) {
				NC_Long dd=new NC_Long(key);
				dd.get(_dbm);
				_hash.put(key, dd);
				return dd.val();
			} else {
				if (d.remover()) {
					return null;
				} else {
					return ((NC_Long) d).val();
				}
			}
		} else {
			return _dbm.getLong(key);
		}
	}
	
	public synchronized void getObject(String key,NDbm2ObjectReader rdr) throws NDbmException {
		if (_caching) {
			NC d=_hash.get(key);
			if (d==null) {
				NC_Blob dd=new NC_Blob(key);
				dd.get(_dbm);
				_hash.put(key, dd);
				getObject(key,rdr);
			} else {
				if (d.remover()) {
					rdr.nildata();
				} else { 
					Blob data=((NC_Blob) d).val();
					if (data == null) {
						rdr.nildata();
					} else {
						NDbmByteArrayInputStream gobj1_bin = new NDbmByteArrayInputStream(data.getData());
						NDbmDataInputStream gobj1_din=new NDbmDataInputStream(gobj1_bin);
						_dbm.readObject(gobj1_din,rdr);
					}
				}
			}
		} else {
			_dbm.getObject(key,rdr);
		}
	}

	public synchronized void putObject(String key, NDbm2ObjectWriter wrt) throws NDbmException {
		if (_caching) {
			NDbmByteArrayOutputStream pobj1_bout = new NDbmByteArrayOutputStream();
			NDbmDataOutputStream pobj1_dout=new NDbmDataOutputStream(pobj1_bout);
			_dbm.writeObject(pobj1_dout,wrt);
			NC_Blob b=new NC_Blob(key,new Blob(key, pobj1_bout.bytes(),pobj1_bout.size()));
			_hash.put(key,b);
		} else {
			_dbm.putObject(key, wrt);
		}
	}
	
	public synchronized void remove(String key) throws NDbmException {
		if (_caching) {
			_hash.put(key, new NC_Remove(key));
		} else {
			_dbm.remove(key);
		}
	}
	
    //////////////////////////////////////////////////////////////////////////////////////////////////
	
	private synchronized NC getNC(String key) {
		return _hash.get(key);
	}
	
	private void flushkey(String key) throws NDbmException {
		NC d=getNC(key);
		if (d!=null) {
			if (!d.flushed()) {
				d.put(_dbm);
			}
		}
	}
	
	public void flushCache() throws NDbmException {
		Set<String> keyset=_hash.keySet();
		Vector<String> vk=new Vector<String>();
		Iterator<String> it=keyset.iterator(); 
		while(it.hasNext()) {
			vk.add(it.next());
		}
		
		_dbm.begin();
		it=vk.iterator();
		while(it.hasNext()) {
			flushkey(it.next());
		}
		_dbm.commit();
	}
	
	public void clearCache() throws NDbmException {
		flushCache();
		_hash.clear();
	}
	
    //////////////////////////////////////////////////////////////////////////////////////////////////
	
	public void close() throws NDbmException {
		clearCache();
		_dbm.close();
		_dbm=null;
		_hash=null;
	}
	
	static public NDbm2Caching openNDbm2(File f,boolean ro) throws NDbmException {
		NDbm2 dbm=NDbm2.openNDbm(f, ro);
		return new NDbm2Caching(dbm);
	}
	
	static public void removeDb(File base) {
		NDbm2.removeDb(base);
	}
}
