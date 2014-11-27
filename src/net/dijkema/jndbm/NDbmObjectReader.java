package net.dijkema.jndbm;

import java.io.DataInput;

public interface NDbmObjectReader {
	public void read(NDbmEncDec db, DataInput din);

	public void nildata();

}
