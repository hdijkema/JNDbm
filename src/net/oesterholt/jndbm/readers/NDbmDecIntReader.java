package net.oesterholt.jndbm.readers;

import net.oesterholt.jndbm.NDbmDecoder;

public class NDbmDecIntReader implements NDbmDecReader<Integer> {
	public Integer read(NDbmDecoder enc) {
		return enc.readInt();
	}
}

