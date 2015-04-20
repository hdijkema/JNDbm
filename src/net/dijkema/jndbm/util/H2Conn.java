package net.dijkema.jndbm.util;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Hashtable;

import net.dijkema.jndbm.util.IConn;
import net.dijkema.jndbm2.exceptions.NDbmAlreadyInUseException;
import net.dijkema.jndbm2.exceptions.NDbmException;

public class H2Conn extends IConn {

	private Connection conn=null;
	private Integer    commitNestCount=0;
	private Long       transactionStartTime=0L;
	private int        transactionCount=0;
	private File       _db_location;  
	private boolean    _readonly;
	
	public String driver() {
		return "H2";
	}
	
	public void shutdown() throws Exception {
		/*try {
			//DriverManager.getConnection("");
		} catch (SQLException e) {
			if (e.getSQLState().equals("XJ015")) {
				// do nothing, this is to be expected
			} else {
				throw new SQLException(e);
			}
		}*/
	}
	
	public Connection getConn() throws NDbmException {
		//Connection conn=newConn();
		try {
			initConn();
		} catch (Exception e) {
			throw new NDbmException(e);
		}
		return conn;
	}
	
	private Connection newConn() throws NDbmException {
		String dbname=_db_location.getAbsolutePath();
		Connection myconn;
		try {
			String h2url="jdbc:h2:"+dbname+"";
			if (_readonly) {
				h2url+=";ACCESS_MODE_DATA=r";
			}
			myconn=DriverManager.getConnection(h2url,"sa",""); 
		} catch (Exception E1) {
			myconn=null;
			throw new NDbmException(E1);
		}
		return myconn;
	}
	
	public boolean hasConn() {
		return conn!=null;
	}
	
	public void initConn() throws Exception {
		if (conn==null) {
			conn=newConn();
		}
	}
	
	public void closeConn() throws Exception {
		while (commitNestCount>0) { commit(); }
		conn.close();
		conn=null;
	}
	
	public void begin() throws Exception {
		if (commitNestCount==0) {
			conn.setAutoCommit(false);
			transactionStartTime=System.currentTimeMillis();
		}
		commitNestCount+=1;
		if (System.currentTimeMillis()>transactionStartTime+(600*1000)) {  // 10 minutes for debugging purposes
			throw new Exception("DerbyConn: Suspected nested transaction going on, taking longer then 60 seconds");
		}
	}
	
	public void commit() throws Exception {
		commitNestCount-=1;
		if (commitNestCount==0) {
			conn.commit();
			conn.setAutoCommit(true);
			//vacuum(false);
		}
		else if (commitNestCount<0) {
			throw new Exception("DerbyConn: Commit count drops below zero!");
		}
		else {
			int time=(120*1000);
			if (System.currentTimeMillis()>transactionStartTime+(time)) { // 10 minutes for debugging purposes
				throw new Exception("DerbyConn: Suspected nested transaction going on, taking longer then 60 seconds");
			}	
		}
	}
	
	public void rollback() throws Exception {
		// TODO: Does nothing.
	}
	
	public int numOfBackupSteps() {
		return 5;
	}
	
	public H2Conn(File db_location,boolean ro) throws NDbmException {
		_db_location=db_location;
		_readonly=ro;
		try {
			Class.forName("org.h2.Driver");
		} catch (ClassNotFoundException e) {
			throw new NDbmException(e);
		}
	}

}
