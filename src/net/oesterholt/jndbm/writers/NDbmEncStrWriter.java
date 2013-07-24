package net.oesterholt.jndbm.writers;

import net.oesterholt.jndbm.NDbmEncoder;

public class NDbmEncStrWriter implements NDbmEncWriter<String> {
	public void write(NDbmEncoder enc, String value) {
		enc.writeString(value);
	}
}

