package net.dijkema.jndbm.writers;

import net.dijkema.jndbm.NDbmEncoder;

public class NDbmEncBoolWriter implements NDbmEncWriter<Boolean> {
	public void write(NDbmEncoder enc, Boolean value) {
		enc.writeBoolean(value);
	}
}

