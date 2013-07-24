package net.oesterholt.jndbm.readers;

import java.awt.Color;

import net.oesterholt.jndbm.NDbmDecoder;

public class NDbmDecColorReader implements NDbmDecReader<Color> {
	public Color read(NDbmDecoder enc) {
		return enc.readColor();
	}
}
