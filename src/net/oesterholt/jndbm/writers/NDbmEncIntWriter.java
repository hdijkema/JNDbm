package net.oesterholt.jndbm.writers;

import net.oesterholt.jndbm.NDbmEncoder;

public class NDbmEncIntWriter implements NDbmEncWriter<Integer> {
	public void write(NDbmEncoder enc, Integer value) {
		enc.writeInt(value);
	}
}

