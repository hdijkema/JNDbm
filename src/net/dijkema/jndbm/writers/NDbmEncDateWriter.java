package net.dijkema.jndbm.writers;

import java.util.Date;

import net.dijkema.jndbm.NDbmEncoder;

public class NDbmEncDateWriter implements NDbmEncWriter<Date> {
	public void write(NDbmEncoder enc, Date value) {
		enc.writeDate(value);
	}
}
