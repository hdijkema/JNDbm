package net.dijkema.jndbm.writers;

import net.dijkema.jndbm.NDbmEncoder;

public class NDbmEncIntWriter implements NDbmEncWriter<Integer> {
	public void write(NDbmEncoder enc, Integer value) {
		enc.writeInt(value);
	}
}

