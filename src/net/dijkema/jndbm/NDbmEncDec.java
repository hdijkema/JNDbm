package net.dijkema.jndbm;

import java.awt.Color;
import java.awt.image.ColorModel;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.Vector;

import net.dijkema.jndbm.datastruct.Types;
import net.dijkema.jndbm.logging.NDbmLogger;
import net.dijkema.jndbm.streams.NDbmDataOutputStream;
import net.dijkema.jndbm2.exceptions.NDbmException;

public class NDbmEncDec {

	static NDbmLogger logger = NDbm.getLogger(NDbmEncDec.class);
	
	private String _context="";
	
	
	// Internal write and read methods

	public void writeInt(DataOutput dout, int g) {
		try {
			dout.writeChar(Types.TYPE_INT);
			dout.writeInt(g);
			/*
			String gg = String.format("%-9s", Integer.toString(g, 16));
			byte[] b = gg.getBytes();
			writeType(bout,TYPE_INT);
			bout.write(b);*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}

	public int readInt(DataInput din) {
		try {
			checkType(din,Types.TYPE_INT);
			return din.readInt();
			/*bin.read(b);
			String s = new String(b);
			Integer r = Integer.parseInt(s.trim(), 16);
			return r;*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return -1;
		}
	}
	
	public void writeColor(DataOutput dout, Color c) {
		try {
			dout.writeChar(Types.TYPE_COLOR);
			int r,g,b,a;
			r=c.getRed();
			g=c.getGreen();
			b=c.getBlue();
			a=c.getAlpha();
			dout.writeShort(r);
			dout.writeShort(g);
			dout.writeShort(b);
			dout.writeShort(a);
		} catch(Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}
	
	public Color readColor(DataInput din) {
		try {
			checkType(din,Types.TYPE_COLOR);
			int r=din.readShort();
			int g=din.readShort();
			int b=din.readShort();
			int a=din.readShort();
			Color c=new Color(r,g,b,a);
			return c;
		} catch(Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return Color.black;
		}
	}

	public void writeLong(DataOutput dout, long g) {
		try {
			dout.writeChar(Types.TYPE_LONG);
			dout.writeLong(g);
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}

	public long readLong(DataInput din) {
		try {
			checkType(din,Types.TYPE_LONG);
			return din.readLong();
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return -1;
		}
	}

	public String readString(DataInput din) {
		try {
			char t=checkType(din,Types.TYPE_STRING,Types.TYPE_NULL_STRING);
			if (t==Types.TYPE_NULL_STRING) {
				return null;
			} else {
				return din.readUTF();
			}
			/*
			char t=checkType(bin,TYPE_STRING,TYPE_STRING_NULL);
			if (t==TYPE_STRING_NULL) {
				return null;
			} else {
				int bytesize = readLength(bin);
				byte[] b = new byte[bytesize];
				bin.read(b);
				String s = new String(b, "UTF-8");
				return s;
			}*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return null;
		}
	}

	public void writeString(DataOutput dout, String s) {
		try {
			if (s==null) {
				dout.writeChar(Types.TYPE_NULL_STRING);
			} else {
				dout.writeChar(Types.TYPE_STRING);
				dout.writeUTF(s);
			}
			/*
			if (s==null) {
				writeType(bout,TYPE_STRING_NULL);
			} else {
				writeType(bout,TYPE_STRING);
				byte[] b = s.getBytes("UTF-8");
				writeLength(bout, b.length);
				bout.write(b);
			}
			*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}

	public void writeDate(DataOutput dout, java.util.Date d) {
		try {
			if (d==null) {
				dout.writeChar(Types.TYPE_NULL_DATE);
			} else {
				dout.writeChar(Types.TYPE_DATE);
				dout.writeLong(d.getTime());
			}
			/*if (d==null) {
				writeType(bout,TYPE_DATE_NULL);
			} else {
				SimpleDateFormat dateFormat = new SimpleDateFormat(
						"yyyyMMdd/HHmmss");
				String s = dateFormat.format(d);
				byte[] b;
				b = s.getBytes();
				if (b.length != 15) {
					logger.fatal("15 expected, got " + b.length + " " + s);
				}
				writeType(bout,TYPE_DATE);
				bout.write(b);
			}*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}

	public java.util.Date readDate(DataInput din) {
		try {
			char t=checkType(din,Types.TYPE_DATE,Types.TYPE_NULL_DATE);
			if (t==Types.TYPE_NULL_DATE) {
				return null;
			} else {
				return new java.util.Date(din.readLong());
			}
			/*	SimpleDateFormat dateFormat = new SimpleDateFormat(
						"yyyyMMdd/HHmmss");
				byte[] b = new byte[15];
			
				bin.read(b);
				String s = new String(b);
				java.util.Date d = dateFormat.parse(s);
				return d;
			}*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return null;
		}
	}

	public Boolean readBoolean(DataInput din) {
		try {
			checkType(din,Types.TYPE_BOOLEAN);
			return din.readBoolean();
			/*
			int b = bin.read();
			if (b == 'F') {
				return false;
			} else {
				return true;
			}*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return null;
		}
	}

	public void writeBoolean(DataOutput dout, boolean b) {
		try {
			dout.writeChar(Types.TYPE_BOOLEAN);
			dout.writeBoolean(b);
			/*int i = (b) ? 'T' : 'F';
			writeType(out,TYPE_BOOLEAN);
			out.write(i);*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}
	
	public void writeType(DataOutput dout,char type) {
		try {
			dout.writeChar(type);
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}
	
	public char readType(DataInput din) {
		try {
			return din.readChar();
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return ' ';
		}
	}
	
	public void writeObject(DataOutput dout,NDbmObjectWriter wrt) {
		try {
			writeType(dout,Types.TYPE_OBJECTWRITER);
			wrt.write(this, dout); 
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}

	public void writeObject(NDbmDataOutputStream dout,NDbm2ObjectWriter wrt) 
									throws NDbmException { 
		writeType(dout,Types.TYPE_OBJECTWRITER);
		wrt.write(this, dout); 
	}
	
	public void readObject(DataInput din,NDbmObjectReader rdr) {
		try {
			checkType(din,Types.TYPE_OBJECTWRITER);
			rdr.read(this, din);
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}

	public void readObject(DataInput din,NDbm2ObjectReader rdr) throws NDbmException {
		checkType(din,Types.TYPE_OBJECTWRITER);
		rdr.read(this, din);
	}
	
	public Vector<String> readVectorOfString(DataInput din) {
		try {
			char t=checkType(din,Types.TYPE_VECTOROFSTRING,Types.TYPE_NULL_VECTOROFSTRING);
			if (t==Types.TYPE_NULL_VECTOROFSTRING) {
				return null;
			} else {
				int N=din.readInt();
				Vector<String> v = new Vector<String>();
				int i;
				for (i = 0; i < N; i++) {
					v.add(din.readUTF());
				}
				return v;
			}
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return null;
		}
	}
	
	public void writeVectorOfString(DataOutput dout,Vector<String> data) {
		try {
			if (data==null) {
				dout.writeChar(Types.TYPE_NULL_VECTOROFSTRING);
			} else {
				dout.writeChar(Types.TYPE_VECTOROFSTRING);
				dout.writeInt(data.size());
				Iterator<String> it = data.iterator();
				while (it.hasNext()) {
					dout.writeUTF(it.next());
				}
			}
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
		}
	}
	
	public boolean isType(char t,char expectedType) {
		return t==expectedType;
	}
	
	public char _charPeeked=' ';
	
	public char peekType(DataInput din) {
		try {
			if (_charPeeked!=' ') {
				return _charPeeked;
			} else {
				_charPeeked=din.readChar();
				return _charPeeked;
			}
			/*
			in.mark(1);
			char t=(char) in.read();
			in.reset();
			return t;*/
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return ' ';
		}
	}
	
	public char checkType(DataInput din, char type1) {
		try {
			char t;
			if (_charPeeked==' ') {
				t=din.readChar();
			} else {
				t=_charPeeked;
				_charPeeked=' ';
			}
			if (t!=type1) {
				logger.fatal(_context+":Expected type '"+type1+"', got type '"+t+"'");
			}
			return t;
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return ' ';
		}
	}

	
	public char checkType(DataInput din, char type1, char type2) {
		try {
			char t;
			if (_charPeeked==' ') {
				t=din.readChar();
			} else {
				t=_charPeeked;
				_charPeeked=' ';
			}
			if (t!=type1 && t!=type2) {
				logger.fatal(_context+":Expected type '"+type1+"' or '"+type2+"', got type '"+t+"'");
			}
			return t;
		} catch (Exception E) {
			logger.error(_context+":"+E.getMessage());
			logger.fatal(E);
			return ' ';
		}
	}
	
	public void setContext(String c) {
		_context=c;
	}
	
	public void DbmEncDec() {
	}

	
	
	
}
