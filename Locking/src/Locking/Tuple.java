package hw1;

import java.sql.Types;
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

	private TupleDesc desc;
	private int pageID;
	private int slotID;
	Map<Integer, byte[]> fields;
	//private HashMap<Integer, Integer> fields;

	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		this.desc = t;
		if (t.numFields() > 0) {
			fields = new HashMap<Integer, byte[]>();
		}
	}

	public TupleDesc getDesc() {
		return this.desc;
	}

	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return this.pageID;
	}

	public void setPid(int pid) {
		//your code here
		this.pageID = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		return this.slotID;
	}

	public void setId(int id) {
		this.slotID = id;
	}

	public void setDesc(TupleDesc td) {
		this.desc = td;
	}

	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, byte[] v) {
		fields.put(i, v);

	}

	public byte[] getField(int i) {
		return fields.get(i);
	}

	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
//		String x = "";
//		for(int i = 0; i < fields.size(); i++) {
//			x += fields.get(i) + " ";
//		}
//		return x.trim();
		//your code here
		StringBuilder description = new StringBuilder();
		for (Entry<Integer, byte[]> entry : fields.entrySet()) {
			if(description.length()!=0) {
				description.append(" / ");
			}
			if (desc.getType(entry.getKey())==Type.STRING)
				description.append(new String(entry.getValue()));
			else if (desc.getType(entry.getKey())==Type.INT){
				byte[] bytes = entry.getValue();
				description.append(bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF));
			}
		}
		return description.toString();
	}
}