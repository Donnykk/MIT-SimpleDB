package simpledb;

/**
 * 一个简单的锁,在getPage时会传入该事务获取的锁的类型,
 * 申请一个新锁时会创建一个PageLock对象，创建时指定锁的类型
 */
public class Lock {
    public static final int READ = 0;
    public static final int WRITE = 1;
    private final TransactionId tid;
    private int type;

    Lock(TransactionId tid, int type) {
        this.tid = tid;
        this.type = type;
    }

    public TransactionId getTid() {
        return tid;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
