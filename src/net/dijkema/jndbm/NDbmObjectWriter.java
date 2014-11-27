package net.dijkema.jndbm;

import java.io.DataOutput;

public interface NDbmObjectWriter {
	public void write(NDbmEncDec db, DataOutput dout);

}
