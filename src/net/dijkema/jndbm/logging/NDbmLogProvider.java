package net.dijkema.jndbm.logging;

public interface NDbmLogProvider {
	public NDbmLogger getLogger(Class<?> c);
}
