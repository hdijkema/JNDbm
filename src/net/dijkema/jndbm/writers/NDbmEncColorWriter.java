package net.dijkema.jndbm.writers;

import java.awt.Color;

import net.dijkema.jndbm.NDbmEncoder;

public class NDbmEncColorWriter implements NDbmEncWriter<Color> {
	public void write(NDbmEncoder enc, Color value) {
		enc.writeColor(value);
	}
}
