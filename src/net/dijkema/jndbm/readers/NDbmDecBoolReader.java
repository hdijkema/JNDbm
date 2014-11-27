package net.dijkema.jndbm.readers;

import net.dijkema.jndbm.NDbmDecoder;

public class NDbmDecBoolReader implements NDbmDecReader<Boolean> { 
	public Boolean read(NDbmDecoder enc) {
		return enc.readBoolean();
	}
}

