package net.dijkema.jndbm.readers;

import java.util.Date;

import net.dijkema.jndbm.NDbmDecoder;

public class NDbmDecDateReader implements NDbmDecReader<Date> {
	public Date read(NDbmDecoder enc) {
		return enc.readDate();
	}
}
