package net.dijkema.jndbm.readers;

import net.dijkema.jndbm.NDbmDecoder;

public class NDbmDecIntReader implements NDbmDecReader<Integer> {
	public Integer read(NDbmDecoder enc) {
		return enc.readInt();
	}
}

