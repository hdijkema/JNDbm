package net.dijkema.jndbm2.exceptions;

public class NDbmException extends Exception {
	
	public NDbmException(String msg) {
		super(msg);
	}
	
	public NDbmException() {
		super();
	}
	
	public NDbmException(Exception e) {
		super(e);
	}

}
