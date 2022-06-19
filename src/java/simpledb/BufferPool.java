package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pageMap;
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageMap = new ConcurrentHashMap<>();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        int lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = Lock.READ;
        } else {
            lockType = Lock.WRITE;
        }
        boolean acquiredLock = false;
        long timeout = new Random().nextInt(2000) + 1000;
        long sysTime = System.currentTimeMillis(); //获取系统时间用于判断是否死锁
        while (!acquiredLock) {
            //lockManager获取锁，获取不到抛出异常
            long currentTime = System.currentTimeMillis();
            if (currentTime - sysTime > timeout) {
                throw new TransactionAbortedException(); //采用抛出异常中断事务的方式解决死锁
            }
            try {
                acquiredLock = lockManager.acquireLock(tid, pid, lockType);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (!pageMap.containsKey(pid)) {
            if (pageMap.size() >= numPages) {
                evictPage();
            }
            Page p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageMap.put(pid, p);
        }
        return pageMap.get(pid);
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        return lockManager.isHoldLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        if (commit) {
            flushPages(tid); //提交成功，则完成事务
        } else {
            List<PageId> keys = new ArrayList<>(pageMap.keySet());
            for (PageId pid : keys) {
                Page p = pageMap.get(pid);
                if (p.isDirty() == tid) { //提交失败，需要将脏页恢复
                    DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    Page recoverPage = dbFile.readPage(pid);
                    pageMap.put(pid, recoverPage);
                }
            }
        }
        lockManager.CompleteTransaction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = dbFile.insertTuple(tid, t);
        for (Page p : pageList) {
            p.markDirty(true, tid);
            if (pageMap.size() > numPages)
                evictPage(); //页面过多时需要淘汰
            pageMap.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pageList = dbFile.deleteTuple(tid, t);
        for (Page p : pageList) {
            p.markDirty(true, tid);
            if (pageMap.size() > numPages)
                evictPage();
            pageMap.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for (PageId pid : pageMap.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache, so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        Page p = pageMap.get(pid);
        p.markDirty(false, null);
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        List<PageId> keys = new ArrayList<>(pageMap.keySet());
        for (PageId pid : keys) {
            if (holdsLock(tid, pid)) {
                flushPage(pid);  //当页面被事务锁定时，写入磁盘
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        List<Page> pages = new ArrayList<>(pageMap.values());
        for (Page p : pages) {
            if (p.isDirty() == null) {
                discardPage(p.getId()); //不是脏页，可以淘汰
                return;
            }
        }
        throw new DbException("All dirty!");
    }
}