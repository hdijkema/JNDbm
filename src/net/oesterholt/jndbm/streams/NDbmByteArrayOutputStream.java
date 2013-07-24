package net.oesterholt.jndbm.streams;

import java.io.ByteArrayOutputStream;

public	class NDbmByteArrayOutputStream extends ByteArrayOutputStream {
		public byte[] bytes() {
			return super.buf;
		}
		
		public int size() {
			return super.count;
		}
	}
	
