package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.log.MyLogger;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

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

    public synchronized Boolean acquireLock1(TransactionId tid, PageId pid, Permissions permission){
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

    public synchronized Boolean deadLock(TransactionId tid, PageId pid,Permissions permission) {
        Vector<Lock> lockList = lockMap.get(pid);
        // 当前页面没有事务加锁，不可能发生死锁
        if (lockList == null || lockList.size() == 0){
            return false;
        }

        if (permission.equals(Permissions.READ_ONLY)){
            // 本事务申请共享锁
            if (lockList.size() == 1){
                Lock lock = lockList.get(0);
                // 如果页面已经加锁，并且加锁的是同一事务，不管当前加的是读锁还是写锁
                // 直接返回即可，因为这个资源已经被当前事务占有，不存在死锁问题
                if (lock.getTid().equals(tid)){
                    return false;
                }
                // 如果是其他事务，并且这个事务加的是写锁，那么本事务需要等待这个事务的资源
                // 加入边之后检测是否存在死锁，存在的话要删除边因为资源获取失败
                if (lock.getPermission().equals(Permissions.READ_WRITE)){
                    deadLock.addEdge(tid,lock.getTid());
                    if(deadLock.cycleDetection(lock.getTid())){
                        deadLock.removeEdge(tid,lock.getTid());
                        return true;
                    }
                }
            }

            // 执行到这说明 这个页面上加的都是读锁 本事务加的也是读锁，不可能发生死锁
            return false;
        } else {
            // 申请写锁
            // 如果加的是写锁，分为三种情况可以加锁成功
            // 1.当前页面只有本事务之前加的读锁，那么读锁升级为写锁
            // 2.当前页面只有本事务之前加的写锁，直接加锁成功
            // 3.当前页面没有事务加锁
            // 其他情况全部会加锁失败
            if(lockList.size() == 1){
                // 如果当前只有本事务占有这个页面，不管是读锁还是写锁，都已经获取了资源
                // 所以不可能发生死锁
                Lock lock = lockList.get(0);
                if (lock.getTid().equals(tid)){
                    return false;
                }
            }

            // 如果占有的是其他事务，因为本事务是写锁，那么必定需要等待资源
            // 加入边集，判断是否出现死锁
            Vector<Lock> tmpList = new Vector<>();
            for (Lock lock : lockList) {
                deadLock.addEdge(tid,lock.getTid());
                tmpList.add(lock);
                if(deadLock.cycleDetection(lock.getTid())){
                    for (Lock tmp : tmpList) {
                        deadLock.removeEdge(tid,tmp.getTid());
                    }
                    return true;
                }
            }

            // 执行到这里说明没有死锁，那么申请资源成功，同时返回无死锁标志
            return false;
        }
    }

    public synchronized void waitResources(TransactionId tid, PageId pid){
        Vector<Lock> lockList = lockMap.get(pid);
        if (lockList == null){
            return;
        }

        for (Lock lock : lockList) {
            deadLock.addEdge(tid, lock.getTid());
        }
        //System.out.println("wait:"+deadLock);
    }

    public synchronized void releaseResources(TransactionId tid, PageId pid){
        Vector<Lock> lockList = lockMap.get(pid);
        MyLogger.log.info("release1:"+deadLock);

        for (Lock lock : lockList) {
            deadLock.removeEdge(lock.getTid(),tid);
        }
        MyLogger.log.info("release2:"+deadLock);
    }

    public synchronized Boolean deadLockDetection(TransactionId tid, PageId pid,Permissions permission) {
        //MyLogger.log.info("wait1:"+deadLock);
        Boolean result = deadLock(tid, pid, permission);
        //MyLogger.log.info("wait2:"+deadLock);
        return result;
    }


    public synchronized Boolean acquireLock(TransactionId tid, PageId pid, Permissions permission) {
        Boolean result = acquireLock1(tid,pid,permission);
        if (result){
            releaseResources(tid,pid);
        }
        return result;
    }
}
