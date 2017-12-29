package hw1;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.Iterator;

import hw1.Catalog.Table;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

	private static final int Iterator = 0;
    public int totalPages;
    public HashMap<Integer, HeapPage> cachedPages;   
    public ArrayList<Integer> queue;
    public HashMap<Integer, Set<Integer>> transactionPages;
    
    
//    https://docs.adobe.com/docs/en/spec/jsr170/javadocs/jcr-2.0/javax/jcr/lock/LockManager.html
//    https://examples.javacodegeeks.com/core-java/util/concurrent/locks-concurrent/readwritelock/java-readwritelock-example/
//    


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // your code here
    	this.totalPages = numPages;
    	this.cachedPages = new HashMap<Integer, HeapPage>();
    	this.transactionPages = new HashMap<Integer, Set<Integer>>();
    	this.queue = new ArrayList<Integer>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param tableId the ID of the table with the requested page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public HeapPage getPage(int tid, int tableId, int pid, Permissions perm) throws Exception {
    	HeapPage retrievedPage = Database.getCatalog().getDbFile(tableId).readPage(pid);
    	if(!transactionPages.containsKey(tid)){
    		transactionPages.put(tid, new HashSet<Integer>());
    	}
    	//see if we have it in cache
    	//see if it has any locks on it
    		//two routes for checking
    		//if read, fine to have read
    		//if write, then do a timed while loop to check if no locks again
    	//add locks
    	//return it
    			//if not in cache, see if theres space
    			//check for locks
    			//same as above
    				//call evict method
    	if(cachedPages.containsKey(pid)){
    		HeapPage cachedPage = cachedPages.get(pid);
    		if(cachedPage.anyReadLocks() || cachedPage.anyWriteLocks()){
    			if(perm.permLevel == 0){
    				if(cachedPage.anyWriteLocks()){
    					long startTime = System.currentTimeMillis();
    					boolean noLocks = false;
    					while(!noLocks && (System.currentTimeMillis()-startTime<10000)){
    						if(!cachedPage.anyWriteLocks()){
    							noLocks = true;
    						}
    					}
    					if(noLocks || cachedPage.getWriteLock(tid)){
    						cachedPage.setReadLock(tid);
    						transactionPages.get(tid).add(pid);
        					return cachedPage;
    					}
    					else{
    						throw new Exception();
    					}
    				}
    				else{
    					cachedPage.setReadLock(tid);
    					transactionPages.get(tid).add(pid);
    					return cachedPage;
    				}
    			}
    			if(perm.permLevel == 1){
    				long startTime = System.currentTimeMillis();
					boolean noLocks = false;
					while(!noLocks && (System.currentTimeMillis()-startTime<10000)){
						if(!(cachedPage.anyWriteLocks() && cachedPage.anyReadLocks())){//change to read
							noLocks = true;
						}
					}
					if(noLocks || (cachedPage.getWriteLock(tid) && cachedPage.getReadLock(tid))){
						cachedPage.setReadLock(tid);
						cachedPage.setWriteLock(tid);
						transactionPages.get(tid).add(pid);
    					return cachedPage;
					}
					else{
						throw new Exception();
					}
    			}
    		}
    		else{
    			if(perm.permLevel == 0){
    				cachedPage.setReadLock(tid);
    			}
    			else if(perm.permLevel == 1){
    				cachedPage.setReadLock(tid);
    				cachedPage.setWriteLock(tid);
    			}
    			transactionPages.get(tid).add(pid);
    			return cachedPage;
    		}
    	}
    	else{
    		if(cachedPages.size() < totalPages){
    			if(retrievedPage.anyReadLocks() || retrievedPage.anyWriteLocks()){
        			if(perm.permLevel == 0){
        				if(retrievedPage.anyWriteLocks()){
        					long startTime = System.currentTimeMillis();
        					boolean noLocks = false;
        					while(!noLocks && (System.currentTimeMillis()-startTime<10000)){
        						if(!retrievedPage.anyWriteLocks()){
        							noLocks = true;
        						}
        					}
        					if(noLocks || retrievedPage.getWriteLock(tid)){
        						cachedPages.put(pid, retrievedPage);
        						HeapPage cachedPage = cachedPages.get(pid);
        						cachedPage.setReadLock(tid);
        						queue.add(pid);
        						transactionPages.get(tid).add(pid);
            					return cachedPage;
        					}
        					else{
        						throw new Exception();
        					}
        				}
        				else{
        					
        					cachedPages.put(pid, retrievedPage);
        					HeapPage cachedPage = cachedPages.get(pid);
        					cachedPage.setReadLock(tid);
        					queue.add(pid);
        					transactionPages.get(tid).add(pid);
        					return cachedPage;
        				}
        				
        			}
        			if(perm.permLevel == 1){
        				long startTime = System.currentTimeMillis();
    					boolean noLocks = false;
    					while(!noLocks && (System.currentTimeMillis()-startTime<10000)){
    						if(!(retrievedPage.anyWriteLocks() && retrievedPage.anyReadLocks())){
    							noLocks = true;
    						}
    					}
    					if(noLocks || (retrievedPage.getWriteLock(tid)&&retrievedPage.getReadLock(tid))){
    						cachedPages.put(pid, retrievedPage);
    						HeapPage cachedPage = cachedPages.get(pid);
    						cachedPage.setReadLock(tid);
    						cachedPage.setWriteLock(tid);
    						queue.add(pid);
    						transactionPages.get(tid).add(pid);
        					return cachedPage;
    					}
    					else{
    						throw new Exception();
    					}
        			}
        			
        		}
        		else{
        			cachedPages.put(pid, retrievedPage);
        			HeapPage cachedPage = cachedPages.get(pid);
        			if(perm.permLevel == 0){
        				cachedPage.setReadLock(tid);
        			}
        			else if(perm.permLevel == 1){
        				cachedPage.setReadLock(tid);
        				cachedPage.setWriteLock(tid);
        			}
        			
        			queue.add(pid);
        			transactionPages.get(tid).add(pid);
        			return cachedPage;
        		}
    		}
    		else{
    				this.evictPage();
    				if(retrievedPage.anyReadLocks() || retrievedPage.anyWriteLocks()){
            			if(perm.permLevel == 0){
            				if(retrievedPage.anyWriteLocks()){
            					long startTime = System.currentTimeMillis();
            					boolean noLocks = false;
            					while(!noLocks && (System.currentTimeMillis()-startTime<10000)){
            						if(!retrievedPage.anyWriteLocks()){
            							noLocks = true;
            						}
            					}
            					if(noLocks || retrievedPage.getWriteLock(tid)){
            						cachedPages.put(pid, retrievedPage);
            						HeapPage cachedPage = cachedPages.get(pid);
            						cachedPage.setReadLock(tid);
            						queue.add(pid);
            						transactionPages.get(tid).add(pid);
                					return cachedPage;
            					}
            					else{
            						throw new Exception();
            					}
            				}
            				else{
            					
            					cachedPages.put(pid, retrievedPage);
            					HeapPage cachedPage = cachedPages.get(pid);
            					cachedPage.setReadLock(tid);
            					queue.add(pid);
            					transactionPages.get(tid).add(pid);
            					return cachedPage;
            				}
            				
            			}
            			if(perm.permLevel == 1){
            				long startTime = System.currentTimeMillis();
        					boolean noLocks = false;
        					while(!noLocks && (System.currentTimeMillis()-startTime<10000)){
        						if(!(retrievedPage.anyWriteLocks() && retrievedPage.anyReadLocks())){
        							noLocks = true;
        						}
        					}
        					if(noLocks || (retrievedPage.getWriteLock(tid) && retrievedPage.getReadLock(tid))){
        						cachedPages.put(pid, retrievedPage);
        						HeapPage cachedPage = cachedPages.get(pid);
        						cachedPage.setReadLock(tid);
        						cachedPage.setWriteLock(tid);
        						queue.add(pid);
        						transactionPages.get(tid).add(pid);
            					return cachedPage;
        					}
        					else{
        						throw new Exception();
        					}
            			}
            			
            		}
            		else{
            			cachedPages.put(pid, retrievedPage);
            			HeapPage cachedPage = cachedPages.get(pid);
            			if(perm.permLevel == 0){
            				cachedPage.setReadLock(tid);
            			}
            			else if(perm.permLevel == 1){
            				cachedPage.setReadLock(tid);
            				cachedPage.setWriteLock(tid);
            			}
            			queue.add(pid);
            			transactionPages.get(tid).add(pid);
            			return cachedPage;
            		}
    			
    		}
    	}
    	return null;
    }
    
 

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param tableID the ID of the table containing the page to unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(int tid, int tableId, int pid) {
    	this.cachedPages.get(pid).setReadUnlock(tid);
    	this.cachedPages.get(pid).setWriteUnlock(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(int tid, int tableId, int pid) {
    	HeapPage checkPage = this.cachedPages.get(pid);
    	if(!cachedPages.containsKey(pid)){
    		checkPage = Database.getCatalog().getDbFile(tableId).readPage(pid);
    	}
    	//see if its in cache, if not use page in database
    	//ask shook if when we call this, it will always be in our cached pages
    	if(checkPage.getReadLock(tid) || checkPage.getWriteLock(tid)){
    		return true;
    	}
    	else{
    		return false;
    	}
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction. If the transaction wishes to commit, write
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(int tid, boolean commit)
        throws IOException {
        // your code here
    	//can access releasePage
    	//if true
    	//remove tid from hashmap

    	Set<Integer> pageIds = this.transactionPages.get(tid);
    	int pid = 0;

    	Iterator<Integer> pidsList = pageIds.iterator();

    	while(pidsList.hasNext()){
    		pid = pidsList.next();
    		int tableId = this.cachedPages.get(pid).getTableId();
    		this.releasePage(tid, tableId, pid);
    		if(commit){
    			this.flushPage(tableId, pid);
    		}
    		else {
    			if(this.cachedPages.get(pid).getDirt()){
    				HeapPage replacementPage = Database.getCatalog().getDbFile(tableId).readPage(pid);
    				HeapPage removalPage = cachedPages.get(pid);
    				cachedPages.remove(pid, removalPage);
    				cachedPages.put(pid, replacementPage);
    			}
    		}
    		this.releasePage(tid, tableId, pid);
    	}

    	
    	
//    	Set<Integer> pageIds = this.transactionPages.get(tid);
//    	int pid = 0;
//
//    	Iterator<Integer> pidsList = pageIds.iterator();
//    	while(pidsList.hasNext()){
//    		pid = pidsList.next();
//    	int tableId = this.cachedPages.get(pid).getTableId();
//    	this.releasePage(tid, tableId, pid);
//    	if(commit){
//    		this.flushPage(tableId, pid);
//    	}
//    	else{
//    		if(this.cachedPages.get(pid).getDirt()){
//    			HeapPage replacementPage = Database.getCatalog().getDbFile(tableId).readPage(pid);
//    			HeapPage removalPage = cachedPages.get(pid);
//    			cachedPages.remove(pid, removalPage);
//    			cachedPages.put(pid, replacementPage);
//    		}
//    	}
//    	this.releasePage(tid, tableId, pid);
//    	}
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to. May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(int tid, int tableId, Tuple t) throws Exception {
        // your code here
    	int pid = t.getPid();
    	HeapPage tuplePage = this.cachedPages.get(pid);
    	if(cachedPages.get(pid).getWriteLock(tid)) {
//    		Database.getCatalog().getDbFile(tableId).addTuple(t);
    		tuplePage.addTuple(t);
    		tuplePage.setDirty();
    		cachedPages.put(pid, tuplePage);
    	}
    	else {
    		throw new Exception();
    	}
//    	HeapPage dirtiedPage = Database.getCatalog().getDbFile(tableId).addTuple(t);
//    	int pid = dirtiedPage.getId();
//    	if(cachedPages.get(pid).getWriteLock(tid)){
//    		dirtiedPage.setDirty();
//    		cachedPages.put(pid, dirtiedPage);
//    	}
//    	else{
//    		throw new Exception();
//    	}

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty.
     *
     * @param tid the transaction adding the tuple.
     * @param tableId the ID of the table that contains the tuple to be deleted
     * @param t the tuple to add
     */
    public  void deleteTuple(int tid, int tableId, Tuple t) throws Exception {
        // your code here

    	int pid = t.getPid();
    	HeapPage tuplePage = this.cachedPages.get(pid);
    	if(cachedPages.get(pid).getWriteLock(tid)) {
//    		Database.getCatalog().getDbFile(tableId).deleteTuple(t);
    		tuplePage.deleteTuple(t);
    		tuplePage.setDirty();
    		cachedPages.put(pid, tuplePage);
    	}
    	else {
    		throw new Exception();
    	}
    }

    private synchronized  void flushPage(int tableId, int pid) throws IOException {
        // your code here
    	//mark heap page as clean
    	HeapPage flushedPage = cachedPages.get(pid);
    	flushedPage.setClean();
    	Database.getCatalog().getDbFile(tableId).writePage(flushedPage);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws Exception {
    	// your code here
    	boolean foundCleanOne = false;
    	for(int i = 0; i < queue.size(); i ++){
    		while(!foundCleanOne){
    			int pid = queue.get(i);
    			if(!cachedPages.get(pid).isDirty){
    				cachedPages.remove(pid);
    				foundCleanOne = true;
    			}
    		}
    	}
    	if(!foundCleanOne){
    		throw new Exception();
    	}
    	
    }

}
