package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {

	public static final int PAGE_SIZE = 4096;
	private File f;
	private TupleDesc td;
	private int id;

	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.f = f;
		this.td = type;
		this.id = this.f.hashCode();
	}

	public File getFile() {
		//your code here
		return this.f;
	}

	public TupleDesc getTupleDesc() {
		//your code here
		return this.td;
	}

	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 * @throws IOException 
	 */
	public HeapPage readPage(int id) {
		//your code here
		try {
			RandomAccessFile filepath = new RandomAccessFile(this.f, "r");
			byte[] b = new byte[PAGE_SIZE];
			int startingPoint = id * PAGE_SIZE;
			filepath.seek(startingPoint);
			filepath.readFully(b);
			filepath.close();
			return new HeapPage(id, b, this.getId());
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IllegalArgumentException();
		} catch (IOException e) {
			System.err.println("Caught IOException: " + e.getMessage());
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.id;
	}

	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws IOException 
	 */
	public void writePage(HeapPage p) throws IOException {
		//your code here
		RandomAccessFile filepath = new RandomAccessFile(this.f, "rw");
		int startingPoint = p.getId() * PAGE_SIZE;
		byte[] newData = p.getPageData();
		filepath.seek(startingPoint);
		filepath.write(newData);
		filepath.close();
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception 
	 */
	public HeapPage addTuple(Tuple t) throws Exception {
		//your code here
		for (int i = 0; i < this.getNumPages(); i++){
			HeapPage heapPage = this.readPage(i);
			for (int j = 0; j < heapPage.getNumSlots(); j++){
				if (!heapPage.slotOccupied(j)){
					heapPage.addTuple(t);
					this.writePage(heapPage);
					System.out.println("added tuple");
					return heapPage;
				}
			}
		}
		byte[] data = new byte[PAGE_SIZE]; 
		HeapPage heapPage = new HeapPage(this.getNumPages(), data, this.getId());
		heapPage.addTuple(t);
		this.writePage(heapPage);
		return heapPage;
	}

	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 * @throws Exception 
	 */
	public void deleteTuple(Tuple t) throws Exception{
		int properPage = t.getPid();
		HeapPage heapPage = this.readPage(properPage);
		Iterator<Tuple> tupleIterator = heapPage.iterator();
		while(tupleIterator.hasNext()){
			System.out.println("check1");
			if (tupleIterator.next().getDesc().equals(t.getDesc())){
				System.out.println("check2");
				heapPage.deleteTuple(t);
				this.writePage(heapPage);
			}
		}
	}

	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 * @throws IOException 
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> all = new ArrayList<Tuple>();
		for (int i = 0; i < this.getNumPages(); i++){
			HeapPage heapPage = this.readPage(i);
			Iterator<Tuple> tupleIterator = heapPage.iterator();
			while(tupleIterator.hasNext()){
				all.add(tupleIterator.next());
			}
		}
		return all;
	}

	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		int fileLength = (int) f.length();
		if ((fileLength % PAGE_SIZE) == 0){
			return fileLength/PAGE_SIZE;
		}
		else{
			return (fileLength/PAGE_SIZE) + 1;
		}
	}
}