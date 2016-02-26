package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    private int fileid;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.fileid = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return this.fileid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs: function of return a heap page
    public Page readPage(PageId pid) {
        HeapPageId  hfid = (HeapPageId) pid;
        byte[] bytes = new byte[BufferPool.PAGE_SIZE];
        try{  
            BufferedInputStream stream = new BufferedInputStream(new FileInputStream(this.file));
            //this is the offset that buffer to skip
            long skipNum = stream.skip(hfid.pageNumber() * BufferPool.PAGE_SIZE);
            int readNum = stream.read(bytes, 0, BufferPool.PAGE_SIZE);
            stream.close();
            if(readNum>BufferPool.PAGE_SIZE){
                throw new IllegalArgumentException("Wrong page ID");
            }
            return new HeapPage(hfid, bytes);    
        }catch(Exception e){
            e.printStackTrace();
        }
        throw new IllegalArgumentException("here?");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        HeapPage hp = (HeapPage)page;
        HeapPageId hpid = hp.getId(); 
        byte[] pageData = hp.getPageData();
        try{
            BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(this.file));
            int offset = hpid.pageNumber() * BufferPool.PAGE_SIZE;
            stream.write(pageData, offset, BufferPool.PAGE_SIZE);
            stream.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int num = (int) Math.ceil(this.file.length() / BufferPool.PAGE_SIZE);
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        if(t==null) throw new DbException("Null tuple to insert");
        BufferPool bp = Database.getBufferPool();
        ArrayList<Page> list = new ArrayList<Page>();

        for(int i=0; i<numPages(); i++){
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage)bp.getPage(tid, pid, Permissions.READ_WRITE);
            if(page.getNumEmptySlots()>0){
                page.insertTuple(t);
                list.add(page);
                break;
            }
        }

        if(list.size()==0){//insertion failure because of no empty slot
            byte[] bytes = HeapPage.createEmptyPageData();//create a new heap page
            PageId pid = new HeapPageId(this.getId(), this.numPages());
            try{
                RandomAccessFile file = new RandomAccessFile(this.file, "rw");
                file.seek(((HeapPageId)pid).pageNumber() * BufferPool.PAGE_SIZE);
                file.write(bytes);
                file.close();
            }catch(Exception e){
                e.printStackTrace();
            }
            HeapPage page = (HeapPage)bp.getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            list.add(page);
        }
        return list;
        
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        if(t==null) throw new DbException("Null tuple to delete");
        BufferPool bp = Database.getBufferPool();
        PageId pid = t.getRecordId().getPageId();
        HeapPage hp = (HeapPage)bp.getPage(tid, pid, Permissions.READ_WRITE);
        hp.deleteTuple(t);
        return hp;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new myFileIterator(tid);
    }

    private class myFileIterator implements DbFileIterator{

        private TransactionId tid;
        private int curPage;
        private Iterator<Tuple> iterator;
        private boolean flag; //to indicate the whether the iterator is open 

        public myFileIterator(TransactionId tid){
            this.curPage = 0;
            this.tid = tid;
            flag = false;
        }

        /**
        * Opens the iterator
        * @throws DbException when there are problems opening/accessing the database.
        */
        public void open() throws DbException, TransactionAbortedException{
            this.flag = true;
            HeapPageId hpid = new HeapPageId(getId(), this.curPage);
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(this.tid, hpid, Permissions.READ_ONLY);
            this.iterator = hp.iterator();
        }

        /** @return true if there are more tuples available. */
        public boolean hasNext() throws DbException, TransactionAbortedException{
            if(!this.flag) return false;
            if(this.iterator==null) return false;
            else if(this.iterator.hasNext()) return true;
            else{
                while(this.curPage<numPages()-1){//heapfile may cause blank pages
                    this.curPage++;
                    HeapPageId hpid = new HeapPageId(getId(), this.curPage);
                    HeapPage hp = (HeapPage) Database.getBufferPool().getPage(this.tid, hpid, Permissions.READ_ONLY);
                    //System.out.println(hp);
                    this.iterator = hp.iterator();
                    if(this.iterator.hasNext()) return true;
                }
            }
            return false;
        }

        /**
        * Gets the next tuple from the operator (typically implementing by reading
        * from a child operator or an access method).
        *
        * @return The next tuple in the iterator.
        * @throws NoSuchElementException if there are no more tuples
        */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            if(!this.flag) throw new NoSuchElementException();
            if(this.hasNext()) return this.iterator.next();
            else throw new NoSuchElementException();
        }

        /**
        * Resets the iterator to the start.
        * @throws DbException When rewind is unsupported.
        */
        public void rewind() throws DbException, TransactionAbortedException{
            this.close();
            this.open();
        }

        /**
        * Closes the iterator.
        */
        public void close(){
            this.flag = false;
            this.curPage = 0;
            this.iterator = null;
        }
    }
}

