package net.dijkema.jndbm.readers;

import java.awt.Color;

import net.dijkema.jndbm.NDbmDecoder;

public class NDbmDecColorReader implements NDbmDecReader<Color> {
	public Color read(NDbmDecoder enc) {
		return enc.readColor();
	}
}
