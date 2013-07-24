package net.oesterholt.jndbm.writers;

import java.util.Date;

import net.oesterholt.jndbm.NDbmEncoder;

public class NDbmEncDateWriter implements NDbmEncWriter<Date> {
	public void write(NDbmEncoder enc, Date value) {
		enc.writeDate(value);
	}
}
