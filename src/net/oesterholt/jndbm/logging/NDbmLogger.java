package net.oesterholt.jndbm.logging;

public class NDbmLogger {
	
	@SuppressWarnings("unchecked")
	public static NDbmLogger getLogger(Class c) {
		final String name=c.getName();
		return new NDbmLogger(name);
	}
	
	private String name;
	
	public NDbmLogger(String nm) {
		name=nm;
	}
	
	public NDbmLogger() {
		name=this.getClass().getName();
	}
		
	private void internal_ndbm_log(String severity,Object msg) {
		System.out.println("class:"+name+":"+severity+":"+msg);
	}
			
	public void debug(Object msg) {
		internal_ndbm_log("DEBUG",msg);
	}

	public void error(Object msg) {
		internal_ndbm_log("ERROR",msg);
	}

	public void fatal(Object msg) {
		internal_ndbm_log("FATAL",msg);
	}

	public void info(Object msg) {
		internal_ndbm_log("INFO ",msg);
	}

	public void warn(Object msg) {
		internal_ndbm_log("WARN ",msg);
	}
}
