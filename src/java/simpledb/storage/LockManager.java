package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

// two kind lock, write lock or read lock
class Lock {
    private TransactionId tid;
    private Permissions permission;

    public Lock(TransactionId tid,Permissions permission){
        this.tid = tid;
        this.permission = permission;
    }

    public void setPermission(Permissions permission){
        this.permission = permission;
    }

    public void setTid(TransactionId tid){
        this.tid = tid;
    }

    public Permissions getPermission(){
        return this.permission;
    }

    public TransactionId getTid(){
        return this.tid;
    }
}

public class LockManager {
    private final ConcurrentHashMap<PageId, Vector<Lock>> lockMap;

    public LockManager(){
        lockMap = new ConcurrentHashMap<>();
    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pid, Permissions permission){
        Vector<Lock> lockList = lockMap.get(pid);
        // page have no lock
        if (lockList == null){
            lockList = new Vector<>();
            lockList.add(new Lock(tid,permission));
            this.lockMap.put(pid,lockList);
            return true;
        }

        if (permission.equals(Permissions.READ_ONLY)){
            // 申请共享锁
            if (lockList.size() == 1){
                // 如果页面已经加锁，并且加锁的是同一事务，那么直接返回加锁成功
                if (lockList.get(0).getTid().equals(tid)){
                    return true;
                }
                // 如果是其他事务，是写锁需要返回失败
                if (lockList.get(0).getPermission().equals(Permissions.READ_WRITE)){
                    return false;
                }
            }
            
            // 这里面的锁都是读锁，如果当前事务对当前页面已经加过读锁 直接返回true
            for (Lock lock : lockList) {
                if (lock.getTid().equals(tid)){
                    return true;
                }
            }

            // 如果没有加过那么添加到链表尾部 返回true
            lockList.add(new Lock(tid,permission));
            lockMap.put(pid,lockList);
            return true;
        } else {
            // 如果加的是写锁，分为三种情况可以加锁成功
            // 1.当前页面只有本事务之前加的读锁，那么读锁升级为写锁
            // 2.当前页面只有本事务之前加的写锁，直接加锁成功
            // 3.当前页面没有事务加锁
            // 其他情况全部会加锁失败
            if(lockList.size() == 1){
                Lock lock = lockList.get(0);
                if (lock.getTid().equals(tid)){
                    if (lock.getPermission().equals(Permissions.READ_ONLY)){
                        // 升级写锁
                        lock.setPermission(Permissions.READ_WRITE);
                    }
                    return true;
                }else{
                    return false;
                }
            }

            return false;
        }

    }

    public synchronized void releaseLock(TransactionId tid, PageId pid){
        Vector<Lock> lockList = lockMap.get(pid);
        if (lockList == null || lockList.size() == 0){
            return;
        }

        for (int i = 0; i < lockList.size(); i++) {
            if (lockList.get(i).getTid().equals(tid)) {
                lockList.remove(i);
                // todo
                if (lockList.size() == 0){
                    lockMap.remove(pid);
                }
                break;
            }
        }
    }

    public synchronized Boolean holdsLock(TransactionId tid, PageId pid){
        Vector<Lock> lockList = lockMap.get(pid);
        if (lockList == null || lockList.size() == 0){
            return false;
        }

        for (Lock lock : lockList) {
            if (lock.getTid().equals(tid)) {
                return true;
            }
        }

        return false;
    }
}
