// John Xiahou
// Deeksha Chaudhary

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
	
	private File file;
	private TupleDesc td;
	private int id;
	
	public static final int PAGE_SIZE = 4096;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		file = f;
		td = type;
		id = f.getAbsoluteFile().hashCode();
	}
	
	public File getFile() {
		//your code here
		return file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return td;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		byte[] data = new byte[PAGE_SIZE];
		RandomAccessFile pageFile = null;
		try {
			pageFile = new RandomAccessFile(file, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			pageFile.seek(PAGE_SIZE*id);
			pageFile.read(data, 0, PAGE_SIZE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			return new HeapPage(id, data, this.id);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return id;
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws FileNotFoundException 
	 */
	public void writePage(HeapPage p) throws FileNotFoundException {
		//your code here
		RandomAccessFile pageFile = new RandomAccessFile(file, "rw");
		try {
			pageFile.seek(p.getId()*PAGE_SIZE);
			pageFile.write(p.getPageData(), 0, PAGE_SIZE);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				pageFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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
		HeapPage newPage = null;
		boolean hasOpenSlot = false;
		for (int i = 0; i < this.getNumPages(); i++) {

			HeapPage page = this.readPage(i);
			if (page.hasEmptySlots()) {
				newPage = page;
				hasOpenSlot = true;
				break;
			}
		}
		if (!hasOpenSlot) {
			newPage = new HeapPage(getNumPages() + 1, new byte[PAGE_SIZE], this.getId());
		}
		newPage.addTuple(t);
		this.writePage(newPage);
		return newPage;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 * @throws Exception 
	 */
	public void deleteTuple(Tuple t) throws Exception{
		//your code here
		HeapPage currentPage = this.readPage(t.getPid());
		currentPage.deleteTuple(t);
		this.writePage(currentPage);
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		for (int i=0; i<this.getNumPages(); i++) {
//			if (pages[i]==null) {
//				break;
//			}
			HeapPage page = this.readPage(i);
			Iterator<Tuple> it = page.iterator();
			while(it.hasNext()) {
				tuples.add(it.next());
			}
		}
//		System.out.println(tuples.size()+ " tuples found.");
		return tuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		int n = (int) (file.length()/PAGE_SIZE);
		if (file.length() % PAGE_SIZE >0) {
			n++;
		}
		return n;
	}
}
