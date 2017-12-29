// John Xiahou
// Deeksha Chaudhary

package hw1;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	private TupleDesc td;
	private int pid;
	private int tid;
	private Map<Integer, byte[]> content_map = new HashMap<Integer, byte[]>();
	
	/**
	 * Creates a new tuple with the given td
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		//your code here
		if (t==null) {
			throw new IllegalArgumentException("Tuple(TupleDesc t): t cannot be null.");
		}
		td = t;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return pid;
	}

	public void setPid(int pid) {
		//your code here
		this.pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return tid;
	}

	public void setId(int id) {
		//your code here
		tid = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		this.td = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, byte[] v) {
		//your code here
		content_map.put(i, v);
	}
	
	public byte[] getField(int i) {
		//your code here
		return content_map.get(i);

	}
	
	public boolean equals(Tuple t) {
		if (!t.getDesc().equals(this.getDesc())) {
			return false;
		}
		for (int i=0; i<td.numFields(); i++) {
			if (!this.getField(i).equals(t.getField(i))) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		StringBuilder description = new StringBuilder();
		for (Entry<Integer, byte[]> entry : content_map.entrySet()) {
			if(description.length()!=0) {
				description.append(" / ");
			}
			if (td.getType(entry.getKey())==Type.STRING)
				description.append(new String(entry.getValue()));
			else if (td.getType(entry.getKey())==Type.INT){
				byte[] bytes = entry.getValue();
				description.append(bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF));
			}
		}
		return description.toString();
	}
}
	