package net.oesterholt.jndbm.writers;

import java.util.Date;

import net.oesterholt.jndbm.NDbmEncoder;

public interface NDbmEncWriter<T> {
	public void write(NDbmEncoder enc,T value);
}

