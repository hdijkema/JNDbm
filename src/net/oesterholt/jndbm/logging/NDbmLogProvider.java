package net.oesterholt.jndbm.logging;

public interface NDbmLogProvider {
	public NDbmLogger getLogger(Class<?> c);
}
