package net.oesterholt.jndbm.writers;

import net.oesterholt.jndbm.NDbmEncoder;

public class NDbmEncBoolWriter implements NDbmEncWriter<Boolean> {
	public void write(NDbmEncoder enc, Boolean value) {
		enc.writeBoolean(value);
	}
}

