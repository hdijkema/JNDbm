package net.oesterholt.jndbm.readers;

import net.oesterholt.jndbm.NDbmDecoder;

public class NDbmDecStrReader implements NDbmDecReader<String> {
	public String  read(NDbmDecoder enc) {
		return enc.readString();
	}
}

