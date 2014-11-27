package net.dijkema.jndbm.readers;

import net.dijkema.jndbm.NDbmDecoder;

public interface NDbmDecReader<T> {
	public T read(NDbmDecoder dec);
}

