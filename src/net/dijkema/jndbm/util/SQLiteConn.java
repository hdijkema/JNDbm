package net.dijkema.jndbm.util;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import net.dijkema.jndbm2.exceptions.NDbmException;

import org.sqlite.SQLiteConfig;

public class SQLiteConn extends IConn {

		private Connection conn=null;
		private Integer    commitNestCount=0;
		private Long       transactionStartTime=0L;
		private File       _db_location;  
		private boolean    _readonly;
		
		public String driver() {
			return "sqlite";
		}
		
		public void shutdown() throws Exception {
			try {
				if (conn != null) { conn.close(); }
			} catch (SQLException e) {
				throw new SQLException(e);
			}
		}
		
		public Connection getConn() throws Exception {
			try {
				initConn();
			} catch (Exception e) {
				throw new NDbmException(e);
			}
			return conn;		}
		
		public Connection newConn() throws  NDbmException {
			String dbname=_db_location.getAbsolutePath();
			Connection myconn;
			try {
				SQLiteConfig c = new SQLiteConfig();
				c.setReadOnly(_readonly);
				String sqliteurl="jdbc:sqlite:"+dbname+".db";
				myconn=DriverManager.getConnection(sqliteurl, c.toProperties());
			} catch (Exception E) {
				myconn=null;
				throw new NDbmException(E);
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
				throw new Exception("SQLiteConn: Suspected nested transaction going on, taking longer then 60 seconds");
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
				throw new Exception("SQLiteConn: Commit count drops below zero!");
			}
			else {
				int time=(120*1000);
				if (System.currentTimeMillis()>transactionStartTime+(time)) { // 10 minutes for debugging purposes
					throw new Exception("SQLiteConn: Suspected nested transaction going on, taking longer then 60 seconds");
				}	
			}
		}
		
		public SQLiteConn(File db_location,boolean ro) throws NDbmException {
			_db_location=db_location;
			_readonly=ro;
			try {
				Class.forName("org.sqlite.JDBC");
			} catch (ClassNotFoundException e) {
				throw new NDbmException(e);
			}
		}
		
			
	
}
