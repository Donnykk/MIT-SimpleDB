package simpledb;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 锁管理器
 * 有申请锁、释放锁、查看指定数据页的指定事务是否有锁这三个功能
 */
public class LockManager {
    private final ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Lock>> pageLocks;

    public LockManager() {
        pageLocks = new ConcurrentHashMap<>();
    }

    //申请锁
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, int type) throws InterruptedException, TransactionAbortedException {
        ConcurrentHashMap<TransactionId, Lock> pageLock = pageLocks.get(pid);
        //当前页没有锁，则新建锁并返回true
        if (pageLock == null) {
            Lock lock = new Lock(tid, type);
            pageLock = new ConcurrentHashMap<>();
            pageLock.put(tid, lock);
            pageLocks.put(pid, pageLock);
            return true;
        }
        Lock lock = pageLock.get(tid);
        if (lock == null) { //page上没有tid对应的锁
            Lock oneLock = null;
            for (Lock l : pageLock.values()) {
                oneLock = l;
            }
            if (type == Lock.READ) { //申请读锁
                if (pageLock.size() == 1) { //页面上只有1个锁
                    if (oneLock.getType() == Lock.READ) { //该锁为读锁
                        Lock newLock = new Lock(tid, type);
                        pageLock.put(tid, newLock);
                        pageLocks.put(pid, pageLock);
                        return true;
                    } else if (oneLock.getType() == Lock.WRITE) { //该锁为写锁，等待一段时间
                        wait(100);
                        return false;
                    }
                } else { //页面上有多个锁
                    Lock l = new Lock(tid, type);
                    pageLock.put(tid, l);
                    pageLocks.put(pid, pageLock);
                    return true;
                }
            } else if (type == Lock.WRITE) { //申请写锁
                wait(100);
                return false;
            }
        } else { //page上有tid对应的锁
            if (lock.getType() == Lock.READ) {
                if (type == Lock.READ) { //该锁与申请的锁同类型
                    return true;
                } else if (type == Lock.WRITE) { //申请的是写锁，则升级为写锁
                    if (pageLock.size() > 1) {
                        throw new TransactionAbortedException(); //该页上还有其他事务的锁，抛出异常
                    }
                    lock.setType(Lock.WRITE);
                    pageLock.put(tid, lock);
                    pageLocks.put(pid, pageLock);
                    return true;
                }
            } else if (lock.getType() == Lock.WRITE) {
                return true;
            }
        }
        return true;
    }

    //查看页面是否被事务锁定
    public synchronized boolean isHoldLock(PageId pid, TransactionId tid) {
        ConcurrentHashMap<TransactionId, Lock> pageLock = pageLocks.get(pid);
        if (pageLock == null) {
            return false;
        }
        Lock lock = pageLock.get(tid);
        return lock != null;
    }

    //释放指定页面的指定事务的锁
    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        ConcurrentHashMap<TransactionId, Lock> pageLock = pageLocks.get(pid);
        if (isHoldLock(pid, tid)) { //未被锁定则直接返回
            pageLock.remove(tid);
            if (pageLock.size() == 0) {
                pageLocks.remove(pid);
            }
            this.notifyAll();
        }
    }

    //完成当前事务，释放所有Page上对应事务的锁
    public synchronized void CompleteTransaction(TransactionId tid) {
        Set<PageId> pidSet = pageLocks.keySet(); //获取key的集合，即可直接遍历释放锁
        for (PageId pid : pidSet) {
            releaseLock(pid, tid);
        }
    }
}
