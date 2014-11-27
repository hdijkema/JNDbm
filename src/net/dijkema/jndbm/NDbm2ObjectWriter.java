package net.dijkema.jndbm;

import java.io.DataOutput;

import net.dijkema.jndbm2.exceptions.NDbmException;

public interface NDbm2ObjectWriter {
	public void write(NDbmEncDec db, DataOutput dout) throws NDbmException;
}
