package net.oesterholt.jndbm.logging;

public interface NDbmLogProvider {
	@SuppressWarnings("unchecked")
	public NDbmLogger getLogger(Class c);
}
