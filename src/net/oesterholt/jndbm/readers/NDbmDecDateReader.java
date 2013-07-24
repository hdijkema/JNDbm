package net.oesterholt.jndbm.readers;

import java.util.Date;

import net.oesterholt.jndbm.NDbmDecoder;

public class NDbmDecDateReader implements NDbmDecReader<Date> {
	public Date read(NDbmDecoder enc) {
		return enc.readDate();
	}
}
