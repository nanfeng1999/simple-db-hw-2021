package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.List;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private LockManager lockManager;

    private final LRUCache<Integer,Page> pageCache;

    private final Byte lock = (byte) 0;

    private static final int DEFAULT_TIME_OUT_TH = 30 * 1000;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        if (numPages <= 0){
            pageCache = new LRUCache<>(DEFAULT_PAGES);
            return;
        }
        pageCache = new LRUCache<>(numPages);
        lockManager = new LockManager();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes her
        while (true){
            synchronized (lock){
                if(!lockManager.acquireLock(tid,pid,perm)){
                    lockManager.waitForResources(tid,pid,perm);
                    Thread.yield();
                    lockManager.dealWithPotentialDeadlocks(tid);

                    if (isTimeOutTransaction(tid)) {
                        throw new TransactionAbortedException();
                    }
                }else{
                    break;
                }
            }
        }

        Page page = pageCache.get(pid.hashCode());
        if (page == null) {

            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = file.readPage(pid);
            if (page == null){
                throw new DbException("Can not get the page");
            }
            if (pageCache.isFull()){
                evictPage();
            }
            pageCache.put(pid.hashCode(), page);
        }

        return page;
    }

    private boolean isTimeOutTransaction(TransactionId tid) {
        if (System.currentTimeMillis() - tid.getStartTime() > DEFAULT_TIME_OUT_TH) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            try {
                flushPages(tid);
            }catch (IOException e){
                e.printStackTrace();
            }
        }else{
            restorePages(tid);
        }

        lockManager.removeTransactionLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid,t);
        for (Page dirtyPage : dirtyPages) {
            PageId pid = dirtyPage.getId();
            if (pageCache.get(pid.hashCode()) != null){
                pageCache.put(dirtyPage.getId().hashCode(),dirtyPage);
            } else if (!pageCache.isFull()) {
                pageCache.put(dirtyPage.getId().hashCode(),dirtyPage);
            }else if(pageCache.isFull()){
                evictPage();
                pageCache.put(dirtyPage.getId().hashCode(),dirtyPage);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtyPages = file.deleteTuple(tid,t);
        for (Page dirtyPage : dirtyPages) {
            pageCache.put(dirtyPage.hashCode(),dirtyPage);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Node<Integer, Page> node = pageCache.getHeadNode();
        node = node.getNext();

        while (node != null && node != pageCache.getTailNode()) {
            Page page = node.getValue();
            if (page.isDirty() != null) {
                flushPage(page.getId());
                // todo ?????????????????? ????????????????????????log??????
                // Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
                // page.markDirty(false, null);
            }

            node = node.getNext();
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageCache.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // ???????????????????????? ???????????????????????? setBeforeImage() ??????
        Page page = pageCache.get(pid.hashCode());
        if (page == null){
            throw new IOException("UnCorrected page id");
        }

        // ??????????????? ????????????????????????????????????????????????????????????????????????
        TransactionId dirtier = page.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();// ??????????????????
        }

        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Node<Integer,Page> node = pageCache.getHeadNode();
        node = node.getNext();

        while(node != null && node != pageCache.getTailNode()){
            Page page = node.getValue();
            TransactionId dirtier = page.isDirty();
            // ????????????????????????????????? ??????????????????setBeforeImage?????? ??????????????????
            if (holdsLock(tid,page.getId())){
                Page before = page.getBeforeImage();
                page.setBeforeImage();
                if (dirtier != null && dirtier.equals(tid)){
                    // todo:?????????????????????????????????
                    // page.setBeforeImage();
                    Database.getLogFile().logWrite(dirtier, before, page);
                    Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
                }
            }


            node = node.getNext();
        }
    }

    public synchronized void restorePages(TransactionId tid) {
        Node<Integer,Page> node = pageCache.getHeadNode();
        node = node.getNext();

        while(node != null && node != pageCache.getTailNode()){
            Page page = node.getValue();
            if (tid.equals(page.isDirty())){
                discardPage(page.getId());
                DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                page = file.readPage(page.getId());
                pageCache.put(page.getId().hashCode(),page);
            }

            node = node.getNext();
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Node<Integer,Page> node = pageCache.getTailNode();
        node = node.getPrev();

        while(node != null){
            if (node == pageCache.getHeadNode()){
                throw new DbException("have no clean page");
            }

            if (node.getValue().isDirty() == null){
                pageCache.remove(node.getKey());
                return;
            }
            node = node.getPrev();
        }
    }

}
