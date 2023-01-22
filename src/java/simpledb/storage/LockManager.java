package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.Map;
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
    private final DeadLock deadLock;

    public LockManager(){
        this.lockMap = new ConcurrentHashMap<>();
        this.deadLock = new DeadLock();
    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pid, Permissions permission){
        // System.out.println(Thread.currentThread().getId()+"\t"+printLockMap());
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

    public synchronized void removeTransactionLocks(TransactionId tid){
        for (Map.Entry<PageId, Vector<Lock>> entry : lockMap.entrySet()) {
            entry.getValue().removeIf(lock -> lock.getTid().equals(tid));
            if(entry.getValue().size() == 0){
                lockMap.remove(entry.getKey());
            }
        }
    }

    public synchronized void waitForResources(TransactionId tid, PageId pid,Permissions permission) {
        Vector<Lock> lockList = lockMap.get(pid);
        // 当前页面没有事务加锁，不需要等待资源
        if (lockList == null || lockList.size() == 0){
            return;
        }

        if (permission.equals(Permissions.READ_ONLY)){
            // 本事务申请共享锁
            if (lockList.size() == 1){
                Lock lock = lockList.get(0);
                // 如果页面已经加锁，并且加锁的是同一事务，不管当前加的是读锁还是写锁
                // 直接返回即可，因为这个资源已经被当前事务占有，不存在死锁问题
                if (lock.getTid().equals(tid)){
                    return;
                }
                // 如果是其他事务，并且这个事务加的是写锁，那么本事务需要等待这个事务的资源
                if (lock.getPermission().equals(Permissions.READ_WRITE)){
                    deadLock.addEdge(tid,lock.getTid());
                }
            }

            // 执行到这说明 这个页面上加的都是读锁 本事务加的也是读锁，不可能发生死锁
        } else {
            // 申请写锁
            for (Lock lock : lockList) {
                // 如果存在本事务，要么是本事务之前加了读锁 要么是本事务之前加了写锁 加了写锁只能有一个锁在队列中 直接跳过即可
                // 如果队列中都是读锁，需要等到其他事务的读锁
                if(lock.getTid().equals(tid)) continue;
                deadLock.addEdge(tid,lock.getTid());
            }

            // 执行到这里说明没有死锁，那么申请资源成功，同时返回无死锁标志
        }
        //System.out.println(Thread.currentThread().getId()+"\t"+deadLock);
    }

    public synchronized void dealWithPotentialDeadlocks(TransactionId tid) throws TransactionAbortedException {
        if (deadLock.cycleDetection(tid)){
            // 可以不删除其他顶点到这个顶点的边 因为遍历到了其他定点发现找不到当前删除的这个边 对结果没有任何影响
            deadLock.removeVertex(tid);
            //System.out.println(Thread.currentThread().getId()+"\t"+"dead lock");
            //Database.getBufferPool().transactionComplete(tid, false);
            throw new TransactionAbortedException();
        }
    }

}
