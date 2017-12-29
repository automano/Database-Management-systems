// John Xiahou
// Deeksha Chaudhary

package test;

import static org.junit.Assert.*;

import org.junit.Test;

import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

public class TupleTest {

	@Test
	public void testFieldAccess() {
		Type[] t = new Type[] {Type.INT, Type.STRING};
		String[] c = new String[] {"a", "bs"};
		
		TupleDesc td = new TupleDesc(t, c);
		
		Tuple tup = new Tuple(td);
		
		byte[] f1 = new byte[] {(byte)(Math.random() * 256), (byte)(Math.random() * 256), (byte)(Math.random() * 256), (byte)(Math.random() * 256)};
		byte[] f2 = new byte[] {(byte)(Math.random() * 256), (byte)(Math.random() * 256), (byte)(Math.random() * 256), (byte)(Math.random() * 256)};

		tup.setField(0, f1);
		tup.setField(1, f2);
		
		assertTrue(tup.getField(0).equals(f1));
		assertTrue(tup.getField(1).equals(f2));

	}

	@Test
	public void testPidAccess() {
		Type[] t = new Type[] {Type.INT, Type.STRING};
		String[] c = new String[] {"a", "bs"};
		
		TupleDesc td = new TupleDesc(t, c);
		Tuple tup = new Tuple(td);
		int pid = 54523665;
		
		tup.setPid(pid);
		
		assertTrue(tup.getPid() == pid);
	}


	@Test
	public void testIdAccess() {
		Type[] t = new Type[] {Type.INT, Type.STRING};
		String[] c = new String[] {"a", "bs"};
		
		TupleDesc td = new TupleDesc(t, c);
		Tuple tup = new Tuple(td);
		int id = 4545542;
		
		tup.setId(id);
		
		assertTrue(tup.getId() == id);
	}

	@Test
	public void testDescAccess() {
		Type[] t = new Type[] {Type.INT, Type.STRING};
		String[] c = new String[] {"a", "bs"};
		
		TupleDesc td = new TupleDesc(t, c);
		Tuple tup = new Tuple(td);
		
		tup.setDesc(td);
		
		assertTrue(tup.getDesc() == td);
	}

	
}
