package net.oesterholt.jndbm.readers;

import net.oesterholt.jndbm.NDbmDecoder;

public interface NDbmDecReader<T> {
	public T read(NDbmDecoder dec);
}

