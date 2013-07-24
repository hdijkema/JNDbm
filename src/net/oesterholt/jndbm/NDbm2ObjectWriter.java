package net.oesterholt.jndbm;

import java.io.DataOutput;

import net.oesterholt.jndbm2.exceptions.NDbmException;

public interface NDbm2ObjectWriter {
	public void write(NDbmEncDec db, DataOutput dout) throws NDbmException;
}
