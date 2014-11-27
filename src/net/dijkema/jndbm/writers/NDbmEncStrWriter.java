package net.dijkema.jndbm.writers;

import net.dijkema.jndbm.NDbmEncoder;

public class NDbmEncStrWriter implements NDbmEncWriter<String> {
	public void write(NDbmEncoder enc, String value) {
		enc.writeString(value);
	}
}

