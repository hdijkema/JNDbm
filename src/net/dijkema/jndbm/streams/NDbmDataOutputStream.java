package net.dijkema.jndbm.streams;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class NDbmDataOutputStream implements DataOutput {

	private OutputStream _out=null;
	
	///////////////////////////////////////////////////////////////////////////////
	
	public NDbmDataOutputStream(OutputStream a) {
		_out=a;
	}

	///////////////////////////////////////////////////////////////////////////////
	
	public void write(int arg0) throws IOException {
		throw new IOException("This method is not implemented");
	}

	public void write(byte[] arg0) throws IOException {
		throw new IOException("This method is not implemented");
	}

	public void write(byte[] arg0, int arg1, int arg2) throws IOException {
		throw new IOException("This method is not implemented");
	}

	///////////////////////////////////////////////////////////////////////////////
	
	public void writeBoolean(boolean b) throws IOException {
		if (b) {
			_out.write('T');
		} else {
			_out.write('F');
		}
	}

	public void writeByte(int arg0) throws IOException {
		throw new IOException("This method is not implemented");
	}

	public void writeBytes(String arg0) throws IOException {
		throw new IOException("This method is not implemented");
	}

	/*
	 * Writes integer out as 4 byte radix 16 counted from 'a'
	 * 
	 * @see java.io.DataOutput#writeChar(int)
	 */
	public void writeChar(int c) throws IOException {
		byte[] b={(byte) ((c&0x3f)+' '),(byte) (((c>>6)&0x3f)+' '),(byte) (((c>>12)&0x3f)+' ')}; // 16 bit /6 ==> 3 ,(byte) (((c>>12)&0xf)+'a')};
		_out.write(b);
	}

	public void writeChars(String s) throws IOException {
		int i,N;
		for(i=0,N=s.length();i<N;i++) {
			writeChar(s.charAt(i));
		}
	}

	public void writeDouble(double d) throws IOException {
		writeLong(Double.doubleToLongBits(d));
	}

	public void writeFloat(float f) throws IOException {
		writeInt(Float.floatToIntBits(f));
	}

	static public byte [] makeInt(int i) {
		byte[] b={(byte) ((i&0x3f)+' '),(byte) (((i>>6)&0x3f)+' '),(byte) (((i>>12)&0x3f)+' '),(byte) (((i>>18)&0x3f)+' '),
				  (byte) (((i>>24)&0x3f)+' '),(byte) (((i>>30)&0x3f)+' ') }; // 32 bit / 6 => 6,(byte) (((i>>24)&0xf)+'a'),(byte) (((i>>28)&0xf)+'a')};
		return b;
	}
	
	public void writeInt(int i) throws IOException {
		_out.write(makeInt(i));
	}

	public void writeLong(long i) throws IOException {
		byte[] b={(byte) ((i&0x3f)+' '),(byte) (((i>>6)&0x3f)+' '),(byte) (((i>>12)&0x3f)+' '),(byte) (((i>>18)&0x3f)+' '),
				  (byte) (((i>>24)&0x3f)+' '),(byte) (((i>>30)&0x3f)+' '),(byte) (((i>>36)&0x3f)+' '),(byte) (((i>>42)&0x3f)+' '),
				  (byte) (((i>>48)&0x3f)+' '),(byte) (((i>>54)&0x3f)+' '),(byte) (((i>>60)&0x3f)+' ')}; //,(byte) (((i>>44)&0x3f)+' '),
				  //(byte) (((i>>48)&0x3f)+'a'),(byte) (((i>>52)&0x3f)+'a'),(byte) (((i>>56)&0x3f)+'a'),(byte) (((i>>60)&0x3f)+'a')};
				  // 64 bit / 6 => 11
		_out.write(b);
	}

	public void writeShort(int s) throws IOException {
		writeChar(s);
	}
	
	public int sizeOfShort() {
		return 3;
	}
	
	public int sizeOfInt() {
		return 6;
	}
	
	public int sizeOfLong() {
		return 11;
	}

	public void writeUTF(String s) throws IOException {
		byte[] b=s.getBytes("UTF-8");
		writeInt(b.length);
		_out.write(b);
	}
}
