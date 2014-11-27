package net.dijkema.jndbm;

import java.io.DataInput;

import net.dijkema.jndbm2.exceptions.NDbmException;

public interface NDbm2ObjectReader {
	public void read(NDbmEncDec db, DataInput din) throws NDbmException;

	public void nildata();
}
