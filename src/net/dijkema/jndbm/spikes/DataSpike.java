package net.dijkema.jndbm.spikes;

import net.dijkema.jndbm.streams.NDbmDataOutputStream;
import net.dijkema.jndbm.streams.NDbmDataInputStream;
import java.io.*;

public class DataSpike {
	
	static public void main(String argv[]) {
		try {
			ByteArrayOutputStream bout=new ByteArrayOutputStream();
			NDbmDataOutputStream dout=new NDbmDataOutputStream(bout);
			
			dout.writeInt(1);
			dout.writeInt(0);
			dout.writeInt(-1);
			dout.writeInt(3309);
			dout.writeInt(-13043420);
			int a=2^31-991;
			dout.writeInt(a);
			int b=-1*2^31+99;
			dout.writeInt(b);
			int c=2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2*2+32321;
			dout.writeInt(c);
			
			dout.writeShort(-121);
			dout.writeShort(0);
			dout.writeShort(1);
			dout.writeShort(-1);
			dout.writeShort(65535);
			dout.writeShort(32767);
			dout.writeShort(32768);
			
			String w="Hallo allemaal, dit is een testje met een string";
			dout.writeUTF(w);
			w="éóöíyÿ and others.";
			dout.writeUTF(w);
			
			long r=230483020492L;
			dout.writeLong(r);
			r=-32497349234324L;
			dout.writeLong(r);
			r=923748937493274233L;
			dout.writeLong(r);  
			
			
			double q=23434.2323112;
			dout.writeDouble(q);
			q=-23212342.23423E-4;
			dout.writeDouble(q);
			
			bout.close();
			String s=bout.toString();
			
			ByteArrayInputStream bin=new ByteArrayInputStream(s.getBytes());
			NDbmDataInputStream din=new NDbmDataInputStream(bin);
			int i=din.readInt();
			i=din.readInt();
			i=din.readInt();
			i=din.readInt();
			i=din.readInt();
			i=din.readInt();
			i=din.readInt();
			i=din.readInt();
			
			short sh=din.readShort();
			sh=din.readShort();
			sh=din.readShort();
			sh=din.readShort();
			sh=din.readShort();
			sh=din.readShort();
			sh=din.readShort();
			
			String ww=din.readUTF();
			ww=din.readUTF();
			
			long R=din.readLong();
			R=din.readLong();
			R=din.readLong();
			
			
			double qq=din.readDouble();
			qq=din.readDouble();
			
			bin.close();
		} catch (Exception E) {
			E.printStackTrace();
		}
		
	}

}
