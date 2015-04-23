package net.dijkema.jndbm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.imageio.ImageIO;

import net.dijkema.jndbm.datastruct.Blob;
import net.dijkema.jndbm.datastruct.Types;
import net.dijkema.jndbm.streams.NDbmByteArrayInputStream;
import net.dijkema.jndbm.streams.NDbmByteArrayOutputStream;
import net.dijkema.jndbm.streams.NDbmDataInputStream;
import net.dijkema.jndbm.streams.NDbmDataOutputStream;
import net.dijkema.jndbm.util.H2Conn;
import net.dijkema.jndbm.util.IConn;
import net.dijkema.jndbm.util.SQLiteConn;
import net.dijkema.jndbm2.exceptions.NDbmException;

public class NDbm2 extends NDbmEncDec {
	
	private IConn _conn=null;
	private int   _MAX_TRANSACTION_DEPTH=20;
	private static Hashtable<File,NDbm2> _existingDbms = new Hashtable<File,NDbm2>();;
	private File _base;
	private boolean _readonly;
	private boolean _h2;
	
	private static boolean _h2type = false;

	//////////////////////////////////////////////////////////////////////////
	// Constructing
	//////////////////////////////////////////////////////////////////////////

	/**
	 * This function opens a database only and only if it is not already opened.
	 * However, for different threads, it keeps different connections.
	 * You *must* call close on the NDbm2 handle in order to clean up your 
	 * connection, otherwise it will stay open in memory.
	 * 
	 * @param base
	 * @param readonly
	 * @return
	 * @throws NDbmException
	 */
	static public NDbm2 openNDbm(File base, boolean readonly) throws NDbmException {
		File dbbase = new File(base.getAbsolutePath()+((_h2type) ? ".h2" : ".db"));
		NDbm2 db=_existingDbms.get(dbbase);
		if (db==null) {
			db=new NDbm2(base, readonly, _h2type);
			_existingDbms.put(dbbase, db);
		} else {
			db = db.copy();
		}
		return db;
	}
	
	static public void setH2() {
		_h2type = true;
	}
	
	static public void setSQLite() {
		_h2type = false;
	}
	
	
	protected NDbm2(File base,boolean ro, boolean h2) throws NDbmException {
		_h2 = h2;
		if (h2) {
			_conn = new H2Conn(base, ro);
		} else {
			_conn = new SQLiteConn(base, ro);
		}
		_base = base;
		_readonly = ro;
		initDb();
	}
	
	protected NDbm2 copy() throws NDbmException {
		return new NDbm2(_base, _readonly, _h2);
	}
	
	public void convertTo(NDbm2 dbTo) throws NDbmException {
		this.begin();
		dbTo.begin();
		Iterator<String> it=this.iterator();
		while (it.hasNext()) {
			String key = it.next();
			Object o = this.getObject(key);
			dbTo.putObject(key, o);
		}
		this.commit();
		dbTo.commit();
	}
	
	public static void removeDb(File _base) {
		/*
		File _meta, _index, _dbase;
		_meta = new File(_base.getAbsolutePath() + ".mta");
		_index = new File(_base.getAbsolutePath() + ".idx");
		_dbase = new File(_base.getAbsolutePath() + ".dbm");
		_meta.delete();
		_index.delete();
		_dbase.delete();*/
		// TODO: CHange to DROP database with H2 
	}
	
	public void close() throws NDbmException {
		// Close will be executed on finalization
	}
	
	protected void finalize() throws Throwable {
		try {
			_mergeBin.close();
			_getBin.close();
			_mergeBlob.close();
			_getBlob.close();
			_mergeInt.close();
			_getInt.close();
			_mergeLong.close();
			_getLong.close();
			_mergeFloat.close();
			_getFloat.close();
			_mergeDouble.close();
			_getDouble.close();
			_mergeString.close();
			_getString.close();
			_mergeBoolean.close();
			_getBoolean.close();
			_getkeys.close();
			_delKey.close();
			_conn.closeConn();
		} catch (SQLException e) {
			throw new NDbmException(e);
		} catch (Exception e) {
			throw new NDbmException(e);
		}
		super.finalize();
	}	
	
	// //////////////////////////////////////////////////////////////////////////////////
	// Info
	// //////////////////////////////////////////////////////////////////////////////////
	
	static public String infoVersion() {
		return "2.02";				// Version of the library
	}

	static public String infoWebSite() {
		return "http://github.com/hdijkema/JNDbm";
	}

	static public String infoLicense() {
		return "LGPL (c) 2009-2015 Hans Dijkema";
	}

	//////////////////////////////////////////////////////////////////////////
	// Transactions
	//////////////////////////////////////////////////////////////////////////
	
	public void setMaxTransactionDepth(int n) {
		_MAX_TRANSACTION_DEPTH=n;
	}
	
	private int _beginCount=0;
	
	public synchronized void begin() throws NDbmException {
		if (_beginCount>0) {
			_beginCount+=1;
			if (_beginCount>_MAX_TRANSACTION_DEPTH) {
				throw new NDbmException(
						"begin() reached Maximum Transaction Depth.\n" +
						"Suspecting endless transaction recursion.\n" +
						"If you know what you're doing, call " +
						"setMaxTransactionDepth() with a higher value than "+_MAX_TRANSACTION_DEPTH
						);
			}
		} else {
			try {
				_begin.execute();
				_beginCount+=1;
			} catch (Exception e) {
				throw new NDbmException(e);
			}
		}
	}
	
	public synchronized void commit() throws NDbmException {
		_beginCount-=1;
		if (_beginCount<0) {
			throw new NDbmException(
					"commit(): transaction count < 0.\n" +
					"You must first start a transaction with begin(),\n" +
					"before committing one."
					);
		} else if (_beginCount==0) {
			try {
				_commit.execute();
			} catch (Exception e) {
				throw new NDbmException(e);
			}
		} else {
			//_beginCount-=1; That's too much
		}
	}
	
	public void rollback() throws NDbmException {
		try {
			_conn.rollback();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	
	
	//////////////////////////////////////////////////////////////////////////
	// Operations
	//////////////////////////////////////////////////////////////////////////
	
	/**
	 * Puts a string in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param data
	 *            - may be null.
	 */
	public void putStr(String key, String data) throws NDbmException {
		writeString(key,data);
	}
	
	/**
	 * Writes a blob to NDbm2, using the given input stream (end point function).
	 * 
	 * @param key
	 * @param in
	 * @throws NDbmException
	 */
	public void putBlob(String key,InputStream in) throws NDbmException {
		writeBlobStream(key,in);
	}

	/**
	 * Writes a blob to NDbm2, using the given a Blob datatype
	 * 
	 * @param key
	 * @param in
	 * @throws NDbmException
	 */
	public void putBlob(String key, Blob data) throws NDbmException {
		//ByteArrayInputStream bin = new ByteArrayInputStream(data.getData());
		//writeBlobStream(key, bin);
		//bin.close();
		this.writeBlob(key, data);
	}
	
	
	/**
	 * Puts a boolean in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param data
	 *            - may be null.
	 */
	public void putBoolean(String key, Boolean data) throws NDbmException {
		writeBoolean(key,data);
	}

	/**
	 * Puts an integer in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param i
	 */
	public void putObject(String key, int i) throws NDbmException {
		putInt(key, i);
	}

	/**
	 * Puts a long in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param i
	 */
	public void putObject(String key, long i) throws NDbmException {
		putLong(key, i);
	}

	/**
	 * Puts a float in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param i
	 */
	public void putObject(String key, float i) throws NDbmException {
		putFloat(key, i);
	}

	/**
	 * Puts a double in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param i
	 */
	public void putObject(String key, double i) throws NDbmException {
		putDouble(key, i);
	}

	
	/**
	 * Puts a boolean in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param b
	 */
	public void putObject(String key, boolean b) throws NDbmException {
		putBoolean(key, b);
	}

	/**
	 * Puts an object in NDbm under the given key (end point function).
	 * 
	 * @param key
	 * @param data - may be null.
	 */
	public void putObject(String key, Object data) throws NDbmException {
		if (data != null) {
			try {
				if (data instanceof Boolean) {
					putBoolean(key, (Boolean) data);
				} else if (data instanceof Integer) {
					putInt(key, (Integer) data);
				} else if (data instanceof String) {
					putStr(key, (String) data);
				} else if (data instanceof Date) {
					putDate(key, (Date) data);
				} else if (data instanceof Long) {
					putLong(key, (Long) data);
				} else if (data instanceof Float) {
					putFloat(key,(Float) data);
				} else if (data instanceof Double) {
					putDouble(key,(Double) data);
				} else {
					NDbmByteArrayOutputStream bout=new NDbmByteArrayOutputStream();
					NDbmDataOutputStream dout = new NDbmDataOutputStream(bout);
					writeType(dout, Types.TYPE_OBJECT);
					ObjectOutputStream oout = new ObjectOutputStream(bout);
					oout.writeObject(data);
					oout.close();
					writeBlob(new Blob(key, bout.bytes(), bout.size()));
				}
			} catch (NDbmException e) {
				throw e;
			} catch (Exception E) {
				throw new NDbmException(E);
			}
		}
	}


	/**
	 * Puts a Vector of String in NDbm under the given key (end point function).
	 * 
	 * @param key
	 * @param data
	 *            - may be null.
	 */
	public void putVectorOfString(String key, Vector<String> data) throws NDbmException {
		if (data != null) {
			try {
				NDbmByteArrayOutputStream bout=new NDbmByteArrayOutputStream();
				NDbmDataOutputStream dout = new NDbmDataOutputStream(bout);
				bout.reset();
				writeVectorOfString(dout, data);
				writeVofS(new Blob(key, bout.bytes(), bout.size()));
			} catch (NDbmException E) {
				throw E;
			} catch (Exception E) {
				throw new NDbmException(E);
			}
		}
	}

	/**
	 * Puts an object in NDbm under the given key (recursable function).
	 * 
	 * @param key
	 * @param wrt
	 */
	public void putObject(String key, NDbm2ObjectWriter wrt) throws NDbmException {
		try {
			NDbmByteArrayOutputStream pobj1_bout = new NDbmByteArrayOutputStream();
			NDbmDataOutputStream pobj1_dout = new NDbmDataOutputStream(pobj1_bout);
			super.writeObject(pobj1_dout, wrt);
			writeBlob(new Blob(key, pobj1_bout.bytes(), pobj1_bout.size()));
		} catch (NDbmException E) {
			throw E;
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}

	/*public void putBlob(String key, Blob b) {
		try {
			writeBlob(b);
		} catch (Exception E) {
			logger.error(_base + ":" + E.getMessage());
			logger.fatal(E);
		}
	}*/


	/**
	 * Puts an Integer in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param data
	 *            - may be null.
	 */
	public void putInt(String key, int data) throws NDbmException {
		writeInt(key,data);
	}

	/**
	 * Puts a Double in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param data
	 *            - may be null.
	 */
	public void putDouble(String key, double data) throws NDbmException {
		writeDouble(key,data);
	}

	/**
	 * Puts a Float in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param data
	 *            - may be null.
	 */
	public void putFloat(String key, float data) throws NDbmException {
		writeFloat(key,data);
	}


	/**
	 * Puts a long in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param data
	 *            - may be null.
	 */
	public void putLong(String key, long data) throws NDbmException {
		writeLong(key,data);
	}

	/**
	 * Puts a Date in NDbm2 under the given key (end point function).
	 * 
	 * @param key
	 * @param data
	 *            - may be null.
	 */
	public void putDate(String key, Date data) throws NDbmException {
		writeDate(key,data);
	}

	/**
	 * Reads an object from NDbm under given key (end point function). Return
	 * value maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public Object getObject(String key) throws NDbmException {
		return readObject(key);
	}

	/**
	 * Reads an object from NDbm2 under given key (recursable function). Return
	 * value maybe null.
	 * 
	 * @param key
	 * @param rdr
	 */
	public void getObject(String key, NDbm2ObjectReader rdr) throws NDbmException {
		try {
			Blob data = readBlob(key);
			if (data == null) {
				rdr.nildata();
			} else {
				NDbmByteArrayInputStream bin = new NDbmByteArrayInputStream(data.getData());
				NDbmDataInputStream din = new NDbmDataInputStream(bin);
				readObject(din, rdr);
			}
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}

	
	/**
	 * Reads an Integer from NDbm under given key (end point function). Return
	 * value maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public Integer getInt(String key) throws NDbmException  {
		return readInt(key);
	}
	
	/**
	 * Reads a BLOB from NDbm under the given key. It returns an InputStream to
	 * the Blob, or null, if the blob was not found.
	 * @param key
	 * @return
	 * @throws NDbmException
	 */
	public InputStream getBlob(String key) throws NDbmException {
		return readBlobStream(key);
	}


	/**
	 * Reads a BLOB from NDbm under the given key. It returns an Blob structure
	 * the Blob, or null, if the blob was not found.
	 * @param key
	 * @return
	 * @throws NDbmException
	 */
	public Blob getBlobData(String key) throws NDbmException {
		return readBlob(key);
	}
	
	
	/**
	 * Reads a Long from NDbm under given key (end point function). Return value
	 * maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public Long getLong(String key) throws NDbmException {
		return readLong(key);
	}

	/**
	 * Reads a a Boolean from NDbm under given key (end point function). Return
	 * value maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public Boolean getBoolean(String key) throws NDbmException {
		return readBoolean(key);
	}

	/**
	 * Reads a a Float from NDbm2 under given key (end point function). Return
	 * value maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public Float getFloat(String key) throws NDbmException {
		return readFloat(key);
	}

	/**
	 * Reads a a Double from NDbm2 under given key (end point function). Return
	 * value maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public Double getDouble(String key) throws NDbmException {
		return readDouble(key);
	}

	/**
	 * Reads a String from NDbm under given key (end point function). Return
	 * value maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public String getStr(String key) throws NDbmException {
		return readString(key);
	}


	/**
	 * Reads a Date from NDbm under given key (end point function). Return value
	 * maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public Date getDate(String key) throws NDbmException {
		return readDate(key);
	}

	/**
	 * Reads a Vector of String from NDbm under given key (end point function).
	 * Return value maybe null.
	 * 
	 * @param key
	 * @return
	 */
	public Vector<String> getVectorOfString(String key) throws NDbmException  {
		Blob b = readVofS(key);
		if (b == null) {
			return null;
		} else {
			return restoreVofS(b);
		}
	}
	
	private Vector<String> restoreVofS(Blob b) throws NDbmException {
		try {
			NDbmByteArrayInputStream gv_bin = new NDbmByteArrayInputStream(b.getData());
			NDbmDataInputStream gv_din = new NDbmDataInputStream(gv_bin);
			return super.readVectorOfString(gv_din);
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}

	/**
	 * Removes the given key from NDbm.
	 * 
	 * @param key
	 */
	public void remove(String key) throws NDbmException {
		removeBlob(key);
	}

	//////////////////////////////////////////////////////////////////////////
	// Iterator
	//////////////////////////////////////////////////////////////////////////
	
	public Iterator<String> iterator() throws NDbmException {
		Vector<String> keys=readKeys();
		return keys.iterator(); 
	}
	
	public Vector<String> keys() throws NDbmException {
		return readKeys();
	}

	//////////////////////////////////////////////////////////////////////////
	// Backend and initialization
	//////////////////////////////////////////////////////////////////////////

	private PreparedStatement _delKey;
	private PreparedStatement _getkeys;
	
	private PreparedStatement _mergeBin;
	private PreparedStatement _getBin;
	
	private PreparedStatement _mergeVoS;
	private PreparedStatement _getVoS;
	
	private PreparedStatement _mergeBlob;
	private PreparedStatement _getBlob;
	
	private PreparedStatement _mergeString;
	private PreparedStatement _getString;

	private PreparedStatement _mergeInt;
	private PreparedStatement _getInt;
	
	private PreparedStatement _mergeLong;
	private PreparedStatement _getLong;

	private PreparedStatement _mergeDate;
	private PreparedStatement _getDate;

	private PreparedStatement _mergeFloat;
	private PreparedStatement _getFloat;
	
	private PreparedStatement _mergeDouble;
	private PreparedStatement _getDouble;

	private PreparedStatement _mergeBoolean;
	private PreparedStatement _getBoolean;
	
	private PreparedStatement _getObject;

	private PreparedStatement _begin;
	private PreparedStatement _commit;
	
	private void initDb() throws NDbmException {
		try {
 			Connection conn=_conn.getConn();
			Statement stmt=conn.createStatement();
			try {
				stmt.execute("create table if not exists dbm (" +
						            "key varchar primary key, " +
									"_bin varbinary default null," +
									"_vos varbinary default null, " +
									"_blob blob default null, " +
									"_int integer default null, " +
									"_long bigint default null, " +
									"_float real default null, " +
									"_double double default null, " +
									"_string varchar default null, " +
									"_timestamp timestamp default null, " +
									"_boolean boolean default null" +
									");");
				stmt.close();
			} catch (Exception E) {
				E.printStackTrace();
				// do nothing
			}
			
			_begin=conn.prepareStatement("begin");
			_commit=conn.prepareStatement("commit");

			_delKey=conn.prepareStatement("delete from dbm where key=?");
			_getkeys=conn.prepareStatement("select key from dbm");

			String merge = "merge into ";
			if (_conn.driver().equals("sqlite")) { merge = "insert or replace into "; } 
			
			_mergeBin=conn.prepareStatement(merge + " dbm (key,_bin) values (?,?)");
			_getBin=conn.prepareStatement("select _bin as value from dbm where key=?");

			_mergeVoS=conn.prepareStatement(merge + " dbm (key,_vos) values (?,?)");
			_getVoS=conn.prepareStatement("select _vos as value from dbm where key=?");

			_mergeBlob=conn.prepareStatement(merge + " dbm (key,_blob) values (?,?)");
			_getBlob=conn.prepareStatement("select _blob as value from dbm where key=?");
			
			_mergeInt=conn.prepareStatement(merge +" dbm (key,_int) values (?,?)");
			_getInt=conn.prepareStatement("select _int as value from dbm where key=?");

			_mergeBoolean=conn.prepareStatement(merge + " dbm (key,_boolean) values (?,?)");
			_getBoolean=conn.prepareStatement("select _boolean as value from dbm where key=?");
			
			_mergeString=conn.prepareStatement(merge + " dbm (key,_string) values (?,?)");
			_getString=conn.prepareStatement("select _string as value from dbm where key=?");

			_mergeFloat=conn.prepareStatement(merge + " dbm (key,_float) values (?,?)");
			_getFloat=conn.prepareStatement("select _float as value from dbm where key=?");

			_mergeLong=conn.prepareStatement(merge + " dbm (key,_long) values (?,?)");
			_getLong=conn.prepareStatement("select _long as value from dbm where key=?");
			
			_mergeDouble=conn.prepareStatement(merge + " dbm (key,_double) values (?,?)");
			_getDouble=conn.prepareStatement("select _double as value from dbm where key=?");

			_mergeDate=conn.prepareStatement(merge + " dbm (key,_timestamp) values (?,?)");
			_getDate=conn.prepareStatement("select _timestamp as value from dbm where key=?");
			
			_getObject=conn.prepareStatement("select * from dbm where key=?");
			
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	private void writeString(String key,String i) throws NDbmException {
		try {
			this.begin();
			_mergeString.setString(1,key);
			_mergeString.setString(2, i);
			_mergeString.execute();
			this.commit();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	private void writeFloat(String key,Float i) throws NDbmException {
		try {
			this.begin();
			_mergeFloat.setString(1,key);
			_mergeFloat.setFloat(2, i);
			_mergeFloat.execute();
			this.commit();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	private void writeBoolean(String key,Boolean i) throws NDbmException {
		try {
			this.begin();
			_mergeBoolean.setString(1,key);
			_mergeBoolean.setBoolean(2, i);
			_mergeBoolean.execute();
			this.commit();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}


	
	private void writeDouble(String key,Double i) throws NDbmException {
		try {
			this.begin();
			_mergeDouble.setString(1,key);
			_mergeDouble.setDouble(2, i);
			_mergeDouble.execute();
			this.commit();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}

	private void writeDate(String key,Date i) throws NDbmException {
		try {
			this.begin();
			_mergeDate.setString(1,key);
			Timestamp ts=new Timestamp(i.getTime());
			_mergeDate.setTimestamp(2, ts);
			_mergeDate.execute();
			this.commit();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}


	private void writeLong(String key,Long i) throws NDbmException {
		try {
			this.begin();
			_mergeLong.setString(1,key);
			_mergeLong.setLong(2, i);
			_mergeLong.execute();
			this.commit();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	
	private void writeInt(String key,Integer i) throws NDbmException {
		try {
			this.begin();
			_mergeInt.setString(1,key);
			_mergeInt.setInt(2, i);
			_mergeInt.execute();
			this.commit();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	private void writeBlob(Blob b) throws NDbmException {
		writeBlob(b.key(), b);
	}

	private void writeBlob(String key, Blob b) throws NDbmException {
		try {
			this.begin();
			_mergeBin.setString(1, key);
			_mergeBin.setBytes(2,b.getData());
			_mergeBin.execute();
			this.commit();
			//_mergeBin.close();
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}

	private void writeVofS(Blob b) throws NDbmException {
		try {
			this.begin();
			_mergeVoS.setString(1,b.key());
			_mergeVoS.setBytes(2, b.getData());
			_mergeVoS.execute();
			this.commit();
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}
	
	private void writeBlobStream(String key,InputStream str) throws NDbmException {
		try {
			this.begin();
			_mergeBlob.setString(1, key);
			if (this._h2) {
				_mergeBlob.setBinaryStream(2, str);
			} else {
				ByteArrayOutputStream B=new ByteArrayOutputStream();
				byte [] buf = new byte[10240];
				int len = str.read(buf);
				while (len >= 0) {
					B.write(buf, 0, len);
					len = str.read(buf);
				}
				B.close();
				_mergeBlob.setBytes(2,B.toByteArray());
			}
			_mergeBlob.execute();
			this.commit();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	private Blob readBlob(String key) throws NDbmException {
		try {
			this.begin();
			_getBin.setString(1,key);
			ResultSet set=_getBin.executeQuery();
			if(set.next()) {
				byte []bt=set.getBytes("value");
				set.close();
				this.commit();
				//_getBin.close();
				return new Blob(key,bt,bt.length);
			} else {
				set.close();
				this.commit();
				//_getBin.close();
				return null;
			}
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}
	
	private Blob readVofS(String key) throws NDbmException {
		try {
			this.begin();
			_getVoS.setString(1,key);
			ResultSet set=_getVoS.executeQuery();
			if(set.next()) {
				byte []bt=set.getBytes("value");
				set.close();
				this.commit();
				return new Blob(key,bt,bt.length);
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}
	
	private InputStream readBlobStream(String key) throws NDbmException {
		try {
			this.begin();
			_getBlob.setString(1,key);
			ResultSet set=_getBlob.executeQuery();
			if (set.next()) {
				InputStream bin=set.getBinaryStream("value");
				set.close();
				this.commit();
				return bin;
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}

	private Long readLong(String key) throws NDbmException {
		try {
			this.begin();
			_getLong.setString(1,key);
			ResultSet set=_getLong.executeQuery();
			if (set.next()) {
				Long v=set.getLong("value");
				set.close();
				this.commit();
				return v;
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}

	private Integer readInt(String key) throws NDbmException {
		try {
			this.begin();
			_getInt.setString(1,key);
			ResultSet set=_getInt.executeQuery();
			if (set.next()) {
				Integer v=set.getInt("value");
				set.close();
				this.commit();
				return v;
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	private Float readFloat(String key) throws NDbmException {
		try {
			this.begin();
			_getFloat.setString(1,key);
			ResultSet set=_getFloat.executeQuery();
			if (set.next()) {
				Float v=set.getFloat("value");
				set.close();
				this.commit();
				return v;
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	private Double readDouble(String key) throws NDbmException {
		try {
			this.begin();
			_getDouble.setString(1,key);
			ResultSet set=_getDouble.executeQuery();
			if (set.next()) {
				Double v=set.getDouble("value");
				set.close();
				this.commit();
				return v;
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}

	private String readString(String key) throws NDbmException {
		try {
			this.begin();
			_getString.setString(1,key);
			ResultSet set=_getString.executeQuery();
			if (set.next()) {
				String v=set.getString("value");
				set.close();
				this.commit();
				return v;
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}

	private Boolean readBoolean(String key) throws NDbmException {
		try {
			this.begin();
			_getBoolean.setString(1,key);
			ResultSet set=_getBoolean.executeQuery();
			if (set.next()) {
				Boolean v=set.getBoolean("value");
				set.close();
				this.commit();
				return v;
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}

	private Date readDate(String key) throws NDbmException {
		try {
			this.begin();
			_getDate.setString(1,key);
			ResultSet set=_getDate.executeQuery();
			if (set.next()) {
				Timestamp ts=set.getTimestamp("value");
				Date v=new Date(ts.getTime());
				this.commit();
				return v;
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception e) {
			throw new NDbmException(e);
		}
	}
	
	private Object readObject(String key) throws NDbmException {
		try {
			this.begin();
			_getObject.setString(1,key);
			ResultSet set=_getObject.executeQuery();
			if (set.next()) {
				byte[] bin=set.getBytes("_bin");
				if (!set.wasNull()) {
					set.close();
					this.commit();
					return bin;
				} else {
					byte[] vos=set.getBytes("_vos");
					if (!set.wasNull()) {
						Blob b=new Blob(key,vos,vos.length);
						set.close();
						this.commit();
						return restoreVofS(b);
					} else {
						InputStream blob = null;
						try {
							blob=set.getBinaryStream("_blob");
						} catch (NullPointerException e) {
							//e.printStackTrace();
						}
						if (!set.wasNull()) {
							set.close();
							this.commit();
							return blob;
						} else { 
							Integer i=set.getInt("_int");
							if (!set.wasNull()) {
								set.close();
								this.commit();
								return i;
							} else {
								Long l=set.getLong("_long");
								if (!set.wasNull()) {
									set.close();
									this.commit();
									return l;
								} else {
									Float f=set.getFloat("_float");
									if (!set.wasNull()) {
										set.close();
										this.commit();
										return f;
									} else {
										Double d=set.getDouble("_double");
										if (!set.wasNull()) {
											set.close();
											this.commit();
											return d;
										} else {
											String s=set.getString("_string");
											if (!set.wasNull()) {
												set.close();
												this.commit();
												return s;
											} else {
												Timestamp ts=set.getTimestamp("_timestamp");
												if (!set.wasNull()) {
													Date dt=new Date(ts.getTime());
													set.close();
													this.commit();
													return dt;
												} else {
													Boolean b=set.getBoolean("_boolean");
													if (!set.wasNull()) {
														set.close();
														this.commit();
														return b;
													} else {
														set.close();
														this.commit();
														return null;
													}
												}
											}

										}
									}
								}
							}
						}
					}
				}
			} else {
				set.close();
				this.commit();
				return null;
			}
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}

	private void removeBlob(String key) throws NDbmException {
		try {
			this.begin();
			_delKey.setString(1, key);
			_delKey.execute();
			this.commit();
		} catch (Exception E) {
			throw new NDbmException(E);
		}
	}
	
	private Vector<String> readKeys() throws NDbmException {
		try {
			this.begin();
			ResultSet set=_getkeys.executeQuery();
			Vector<String> keys=new Vector<String>();
			while (set.next()) {
				keys.add(set.getString("key"));
			}
			set.close();
			this.commit();
			return keys;
		} catch(Exception E) {
			throw new NDbmException(E);
		}
	}

	
	
}
