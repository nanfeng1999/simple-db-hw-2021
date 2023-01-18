package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private int tableid;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.tableid = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try{
            if (pid instanceof HeapPageId) {
                HeapPageId hpid = (HeapPageId) pid;

                int pageNumber = hpid.getPageNumber();
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
                byte[] data = new byte[BufferPool.getPageSize()];

                // todo: it is easy to forget that pageNo == 0 ,you don not need skip any bytes
                // todo: (pageNumber - 1) * BufferPool.getPageSize() is wrong
                if (bis.skip((long) pageNumber * BufferPool.getPageSize()) !=
                        (long) pageNumber * BufferPool.getPageSize()) {
                    throw new IllegalArgumentException(
                            "Unable to seek to correct place in HeapFile");
                }
                int ret = bis.read(data, 0, BufferPool.getPageSize());
                if (ret == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (ret < BufferPool.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BufferPool.getPageSize() + " bytes from HeapFile");
                }
                bis.close();
                return new HeapPage(hpid,data);

            }else {
                throw new Exception("PageId is not the instance of HeapPageId");
            }

        }catch(Exception e){
            e.printStackTrace();
        }

        return null;

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        byte[] data = page.getPageData();
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        rf.seek(offset);
        rf.write(data);
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(tableid,i);

            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0){
                page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
                page.insertTuple(t);
                return new ArrayList<>(Arrays.asList(page));
            }else{
                Database.getBufferPool().unsafeReleasePage(tid,pid);
            }
        }

        HeapPage currPage = new HeapPage(new HeapPageId(getId(),numPages()),HeapPage.createEmptyPageData());
        currPage.insertTuple(t);
        writePage(currPage);

        return new ArrayList<>(Arrays.asList(currPage));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != tableid){
            throw new DbException("UnCorrect tableId");
        }

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        page.deleteTuple(t);

        return new ArrayList<>(Arrays.asList(page));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        class innerIter implements DbFileIterator{
            private int pageNo = 0;
            private Iterator<Tuple> it;
            private HeapPage page;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (pageNo > 0){
                    throw new DbException("Can not call open function twice");
                }
                getPageByNo(pageNo);
                pageNo++;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException{
                // todo: it is easy to forget that not open or close,hasNext() return false
                if (page == null || it == null){
                    return false;
                }

                while(pageNo < numPages() && !it.hasNext()){
                    getPageByNo(pageNo);
                    pageNo ++;
                }

                return !(pageNo == numPages() && !it.hasNext());
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                // todo: it is easy to forget that not open or close,hasNext() return false
                if (page == null || it == null){
                    throw new NoSuchElementException("have no element");
                }

                return it.next();

            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                pageNo = 0;
                it = null;
                page = null;
            }

            public void getPageByNo(int pageNo) throws TransactionAbortedException, DbException {
                HeapPageId hpid = new HeapPageId(tableid,pageNo);
                page = (HeapPage) Database.getBufferPool().getPage(tid,hpid,Permissions.READ_ONLY);
                it = page.iterator();
            }
        }

        return new innerIter();
    }

}

