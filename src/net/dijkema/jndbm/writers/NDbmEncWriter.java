package net.dijkema.jndbm.writers;

import java.util.Date;

import net.dijkema.jndbm.NDbmEncoder;

public interface NDbmEncWriter<T> {
	public void write(NDbmEncoder enc,T value);
}

