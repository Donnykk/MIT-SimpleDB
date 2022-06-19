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
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final int id;
    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.id = f.getAbsolutePath().hashCode();
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
        return id;
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
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * pageSize;
        byte[] bytes = new byte[pageSize];
        Page page = null;
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) { //只读方式打开
            raf.seek(offset);
            raf.read(bytes);
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(),
                    pid.getPageNumber());
            page = new HeapPage(heapPageId, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        int pgNo = page.getId().getPageNumber();
        if (pgNo > numPages()) {
            throw new IllegalArgumentException();
        }
        int pgSize = BufferPool.getPageSize();
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.seek((long) pgNo * pgSize);
        byte[] data = page.getPageData();
        file.write(data);
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(id, i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE); //确保读写权限
            if (page.getNumEmptySlots() == 0) {
                //当获取的page没有空的slot时立即释放在这个page的锁
                Database.getBufferPool().releasePage(tid, pid);
                continue;
            }
            page.insertTuple(t);
            list.add(page);
            return list;
        }
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(f, true));
        byte[] data = HeapPage.createEmptyPageData();
        bufferedOutputStream.write(data);
        bufferedOutputStream.close();
        PageId pid = new HeapPageId(id, numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        list.add(heapPage);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }


    private static final class HeapFileIterator implements DbFileIterator {
        private int pgNo;
        private final HeapFile file;
        private final TransactionId tid;
        private Iterator<Tuple> tupleIterator;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.file = file;
            this.tid = tid;
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException {
            if (pageNumber >= 0 && pageNumber < file.numPages()) {
                HeapPageId pid = new HeapPageId(file.getId(), pageNumber);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            } else {
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber, file.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) {
                return false;
            }

            if (!tupleIterator.hasNext()) {
                if (pgNo < (file.numPages() - 1)) {
                    pgNo++;
                    tupleIterator = getPageTuples(pgNo);
                    return tupleIterator.hasNext();
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null || !tupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgNo = 0;
            HeapPage heapPage = (HeapPage) Database.getBufferPool()
                    .getPage(tid, new HeapPageId(file.getId(), pgNo), Permissions.READ_ONLY);
            tupleIterator = heapPage.iterator();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }


        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }
    }
}

