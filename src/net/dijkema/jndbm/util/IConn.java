/*
 * $Id: IConn.java,v 1.4 2009/04/15 21:22:45 cvs Exp $
 * 
 * This source code is copyright of Hans Dijkema.
 * © 2008-2009 Hans Dijkema. All rights reserved.
 * 
 * This source code is property of it's author: Hans Dijkema.
 * Nothing of this code may be copied, (re)used or multiplied without
 * permission of the author. 
*/
package net.dijkema.jndbm.util;


import java.sql.Connection;
import java.util.Date;
import java.util.zip.ZipOutputStream;

public class IConn {

	public String driver() {
		return null;
	}
	
	public void begin()  		throws Exception {
		throw new Exception("Needs to be implemented");
	}
	
	public void commit() 		throws Exception {
		throw new Exception("Needs to be implemented");
	}
	
	public void rollback() 		throws Exception {
		throw new Exception("Needs to be implemented");
	}
	
	public void initConn()      throws Exception {
		throw new Exception("Needs to be implemented");
	}
	
	public void closeConn() 	throws Exception {
		throw new Exception("Needs to be implemented");
	}
	
	public void shutdown() throws Exception {
		throw new Exception("Needs to be implemented");
	}
	
	public Connection getConn()	throws Exception {
		throw new Exception("Needs to be implemented");
	}
	
	public java.sql.Date getdate(Date s) {
		return new java.sql.Date(s.getTime());
	}
	
	public java.util.Date sql2util(java.sql.Date s) {
		return new java.util.Date(s.getTime());
	}
	
	public java.util.Date sql2util(java.sql.Timestamp t) {
		return new java.util.Date(t.getTime());
	}
	
	public java.sql.Timestamp gettimestamp(Date s) {
		return new java.sql.Timestamp(s.getTime());
	}
	
	public boolean hasConn() {
		return false;
	}
	
	public int numOfBackupSteps() {
		return 0;
	}
	
	/*public void backup(Jzc3Progress p,ZipOutputStream str,Directories.EntryMaker em) throws Exception {
		throw new Exception("Needs to be implemented");
	}*/
}
