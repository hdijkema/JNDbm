package net.dijkema.jndbm.readers;

import net.dijkema.jndbm.NDbmDecoder;

public class NDbmDecStrReader implements NDbmDecReader<String> {
	public String  read(NDbmDecoder enc) {
		return enc.readString();
	}
}

