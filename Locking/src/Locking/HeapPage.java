package hw1;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import hw1.Catalog.Table;

public class HeapPage {

	private int id;
	private byte[] header;
	private Tuple[] tuples;
	private TupleDesc td;
	private int numSlots;
	private int tableId;
	public Map<Tuple, Integer> slotIDs;
	public Map<Tuple, Integer> emptyIDs;
	private Map<Integer, Boolean> writeLocks;
	private Map<Integer, Boolean> readLocks;
	public boolean isDirty;



	public HeapPage(int id, byte[] data, int tableId) throws IOException {
		this.id = id;
		this.tableId = tableId;
		this.isDirty = false;

		this.td = Database.getCatalog().getTupleDesc(this.tableId);
		this.numSlots = getNumSlots();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
		slotIDs = new HashMap<Tuple, Integer>();
		emptyIDs = new HashMap<Tuple, Integer>();
		writeLocks = new HashMap<Integer, Boolean>();
		readLocks = new HashMap<Integer, Boolean>();

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		try{
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];
			for (int i=0; i<tuples.length; i++)
				tuples[i] = readNextTuple(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		dis.close();
	}

	public int getId() {
		//your code here
		return this.id;
	}
	
	public int getTableId(){
		return this.tableId;
	}
	
	public void setDirty(){
		this.isDirty = true;
	}
	
	public void setClean(){
		this.isDirty = false;
	}
	
	public boolean getDirt(){
		return isDirty;
	}

	/**
	 * Computes and returns the total number of slots that are on this page (occupied or not).
	 * Must take the header into account!
	 * @return number of slots on this page
	 */
	public int getNumSlots() {
		return (HeapFile.PAGE_SIZE * 8) / (td.getSize() * 8 + 1);
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes (no partial bytes)
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {        
		int numTuples = getNumSlots();
		int headerSize = 0;
		int size1 = numTuples / 8;
		int remainder = numTuples % 8;
		if (remainder != 0){
			headerSize = size1 + 1;
		}
		else{
			headerSize = size1;
		}
		return headerSize;
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * @param s the slot to test
	 * @return true if occupied
	 * @throws Exception 
	 */
	public boolean slotOccupied(int s){
		if(s > this.getNumSlots()){
			return false;
		}
		int byteNum = s/8;
		int pos = s % 8;
		if (byteNum >= header.length){
			return false;
		}
		byte currentByte = header[byteNum];
		byte isOdd = (byte) ((currentByte >> pos) & 1);
		int change = (int) isOdd;
		int test = change % 2;
		if(test == 1){
			return true;
		}
		else{
			return false;
		}
	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * @param s the slot to modify
	 * @param value its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		if(s > this.getNumSlots()){
			return;
		}
		int byteNum = 0;
		if(s < 8){
			byteNum = 0;
		}
		else{
			byteNum = (s/8);
		}
		int pos = s % 8;
		if(pos == 0 && pos != 0){
			byteNum += 1;
		}
		if (byteNum >= header.length){
			return;
		}
		byte currentByte = header[byteNum];
		byte newByte = 0;
		if(value){
			newByte = (byte) (currentByte | (0x01 << pos));
		}
		else{
			newByte = (byte) (currentByte & ~(0x01 << pos));
		}
		header[byteNum] = newByte;
	}

	/**
	 * Adds the given tuple in the next available slot. Throws an exception if no empty slots are available.
	 * Also throws an exception if the given tuple does not have the same structure as the tuples within the page.
	 * @param t the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		if (!this.td.equals(t.getDesc())){
			throw new Exception();
		}
		else{
			boolean noSlotsAvail = true;
			for (int i=0; i<numSlots; i++){
				if (!this.slotOccupied(i)){
					noSlotsAvail = false;
					this.setSlotOccupied(i, true);
					t.setId(i);
					System.out.println("add one" + slotIDs.size());
					slotIDs.put(t, i);
					System.out.println("add one" + slotIDs.size());
					tuples[i] = t;
					break;
				}
			}
			if(noSlotsAvail){
				throw new Exception();
			}
		}
	}



	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does not match this page, throw
	 * an exception. If the tuple slot is already empty, throw an exception
	 * @param t the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) throws Exception {
		if (t.getPid() != this.id){
			throw new Exception();
		}
		int s = t.getId();
		if(!tuples[s].getDesc().equals(t.getDesc())){
			throw new Exception();
		}
		if (!this.slotOccupied(s)){
			throw new Exception();
		}
		this.setSlotOccupied(s, false);
		slotIDs.remove(t, s);
		tuples[s] = null;

	}

	/**
	 * Suck up tuples from the source file.
	 */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i=0; i<td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);
		slotIDs.put(t, slotId);


		for (int j=0; j<td.numFields(); j++) {
			if(td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, field);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, field);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


		return t;
	}
	
	public void setReadLock(int transactionID){
		this.readLocks.put(transactionID, true);
	}
	
	public void setWriteLock(int transactionID){
		this.writeLocks.put(transactionID, true);
	}
	
	public void setReadUnlock(int transactionID){
		this.readLocks.put(transactionID, false);
	}
	
	public void setWriteUnlock(int transactionID){
		this.writeLocks.put(transactionID, false);
	}
	
	public boolean getReadLock(int transactionID){
		return this.readLocks.get(transactionID);
	}
	
	public boolean getWriteLock(int transactionID){
		return this.writeLocks.get(transactionID);
	}
	
	public Iterator<Integer> getReadKeys(){
		Set<Integer> keys = this.readLocks.keySet();
		return keys.iterator();
	}
	
	public Iterator<Integer> getWriteKeys(){
		Set<Integer> keys = this.writeLocks.keySet();
		return keys.iterator();
	}
	
	public boolean anyReadLocks(){
		Iterator<Integer> keys = this.getReadKeys();
		while(keys.hasNext()){
			if(this.getReadLock(keys.next())){
				return true;
			}
		}
		return false;
	}
	
	public boolean anyWriteLocks(){
		Iterator<Integer> keys = this.getWriteKeys();
		while(keys.hasNext()){
			if(this.getWriteLock(keys.next())){
				return true;
			}
		}
		return false;
	}
	

	/**
	 * Generates a byte array representing the contents of this page.
	 * Used to serialize this page to disk.
	 *
	 * The invariant here is that it should be possible to pass the byte
	 * array generated by getPageData to the HeapPage constructor and
	 * have it produce an identical HeapPage object.
	 *
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i=0; i<tuples.length; i++) {

			// empty slot
			if (!slotOccupied(i)) {
				for (int j=0; j<td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j=0; j<td.numFields(); j++) {
				byte[] f = tuples[i].getField(j);
				try {
					dos.write(f);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page. 
	 * @return
	 */
	public Iterator<Tuple> iterator() {
		//your code here
//		Set<Tuple> keys = null;
//		int count = 0;
//		for (int j = 0; j < this.getNumSlots(); j++){
//
//			if (this.slotOccupied(j)){
//				count++;
//			}
//		}
//		if (count == 0){
//			keys = emptyIDs.keySet();
//			return keys.iterator();
//		}
//		else{
//			keys = slotIDs.keySet();
//			return keys.iterator();
//		}

		
		List<Tuple> tupleList = new ArrayList<Tuple>();
		for (int i=0; i<this.tuples.length; i++) {
			if (slotOccupied(i)) {
				tupleList.add(tuples[i]);
			}
		}
		return tupleList.iterator();
	}
}