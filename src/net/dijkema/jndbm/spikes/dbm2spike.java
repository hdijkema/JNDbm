package net.dijkema.jndbm.spikes;

import java.io.File;
import java.util.Iterator;
import java.util.Vector;

import net.dijkema.jndbm.NDbm2;

public class dbm2spike {
	public static void main(String [] args) {
		
		try {
			File f=new File("test");

			// tests to make it work
			
			// Replacement of keys
			int testnr=1;
			System.out.println("test"+testnr++);

			NDbm2 db=NDbm2.openNDbm(f,false);
			db.putStr("test", "Hallo allemaal");
			System.out.println("test="+db.getStr("test"));
			db.putInt("int",100);
			System.out.println("int="+db.getInt("int"));
			Vector<String> v=new Vector<String>();
			v.add("Hi");
			v.add("Well");
			db.putVectorOfString("vector", v);
			System.out.println("vector="+db.getVectorOfString("vector"));
			db.close();
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,true); 
			System.out.println("test="+db.getStr("test"));
			System.out.println("int="+db.getInt("int"));
			System.out.println("vector="+db.getVectorOfString("vector"));
			db.close();
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,false);
			System.out.println("int="+db.getInt("int"));
			db.putStr("test","Nou ja zeg, wat nu dan");
			System.out.println("int="+db.getInt("int"));
			db.close();
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,false);
			System.out.println("int="+db.getInt("int"));
			db.putStr("test","Nou ja zeg, wat nu dan");
			System.out.println("int="+db.getInt("int"));
			db.close();
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,false);
			System.out.println("int="+db.getInt("int"));
			db.putStr("test","Klein");
			db.putInt("new", 80822);
			db.putInt("min", -232232);
			System.out.println("int="+db.getInt("int"));
			db.close();
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,true);
			System.out.println("test="+db.getStr("test"));
			System.out.println("new="+db.getInt("new"));
			System.out.println("min="+db.getInt("min"));
			System.out.println("vector="+db.getVectorOfString("vector"));
			System.out.println("int="+db.getInt("int"));
			db.close();
			
			// Iteration of keys
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,true);
			Iterator<String> it=db.iterator();
			while (it.hasNext()) {
				System.out.println("key: "+it.next());
			}
			db.close();
			
			// Removal of key
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,false);
			Vector<String> k=db.keys();
			db.remove("new");
			Vector<String> k1=db.keys();
			System.out.println("k  contains 'new'? :"+k.contains("new"));
			System.out.println("k1 contains 'new'? :"+k1.contains("new"));
			System.out.println("result of 'new'?   :"+db.getStr("new"));
			db.close();
			
			// Not closing the database
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,true);
			it=db.iterator(); 
			while (it.hasNext()) {
				String key=it.next();
				System.out.println(key+"="+db.getObject(key));
			}
			
			// Finalization
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,false);
			db.putObject("boolean", true);
			db.putObject("tryit", 98213);
			it=db.iterator(); 
			while (it.hasNext()) {
				String key=it.next();
				System.out.println(key+"="+db.getObject(key));
			}
			Runtime.getRuntime().gc();
			
			// Scaling up
			
			System.out.println("test"+testnr++);
			db=NDbm2.openNDbm(f,false);
			int i;
			for(i=0;i<100;i++) {
				System.out.print(i+" ");
				db.putInt("key:"+i, i*4);
			}
			it=db.iterator();
			while (it.hasNext()) {
				System.out.print(it.next()+" ");
			}
			System.out.println();
			db.close();
			
			
			// Two times the same database
			// TODO: Let multiple threads work!
			
			/*System.out.println("test"+testnr++);
			class TstThread extends Thread {
				private NDbm2 db;
				private int  factor;
				public TstThread(NDbm2 d,int f) { db=d;factor=f; }
				public void run() {
					int i;
					for(i=0;i<1000;i++) { 
						System.out.print(i+":"+i*factor+" ");
						if (i%10==0) { System.out.println(); }
						db.putInt("key:"+i, i*factor);
					}
					System.out.println();
				}
			}
			db=NDbm2.openNDbm(f);
			NDbm2 db1=NDbm2.openNDbm(f);
			Thread th1=new TstThread(db,1);
			Thread th2=new TstThread(db1,2);
			th1.start();
			th2.start();
			th1.join();
			th2.join();
			
			it=db.iterator();
			while(it.hasNext()) {
				System.out.print(it.next()+" ");
			}
			System.out.println();
			*/
			
			// optimize test
			
			db=NDbm2.openNDbm(f,false);
			int factor=2;
			for(i=0;i<1000;i++) { 
				System.out.print(i+":"+i*factor+" ");
				if (i%10==0) { System.out.println(); }
				db.putInt("key:"+i, i*factor);
			}
			System.out.println();
			
			
			
		} catch(Exception e) {
			System.out.println(e);
		}
		
		System.exit(0);
	}

}
