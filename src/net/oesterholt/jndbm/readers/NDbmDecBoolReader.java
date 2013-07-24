package net.oesterholt.jndbm.readers;

import net.oesterholt.jndbm.NDbmDecoder;

public class NDbmDecBoolReader implements NDbmDecReader<Boolean> { 
	public Boolean read(NDbmDecoder enc) {
		return enc.readBoolean();
	}
}

