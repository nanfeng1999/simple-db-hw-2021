package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.*;

public class DeadLock {
    // 可以使用set，用来去重，一个事务不应该等待另一个事务两次资源
    // 当事务1等待事务2页面1时，它会悬挂在某个地方，不可能存在事务1还等待事务2页面2的情况，因为它必须获取了页面1才会获取页面2
    private final Map<TransactionId, Set<TransactionId>> cycleDetection;

    public DeadLock() {
        cycleDetection = new HashMap<>();
    }

    public void addEdge(TransactionId s,TransactionId d){
        Set<TransactionId> list = cycleDetection.get(s);
        if (list == null){
            list = new HashSet<>();
            list.add(d);
            cycleDetection.put(s,list);
            return;
        }

        list.add(d);
    }

    public void removeEdge(TransactionId s,TransactionId d){
        Set<TransactionId> list = cycleDetection.get(s);
        if (list == null){
            return;
        }

        list.remove(d);
        if (list.size() == 0){
            cycleDetection.remove(s);
        }
    }

    public void removeVertex(TransactionId tid){
        cycleDetection.remove(tid);

    }

    public Boolean cycleDetection(TransactionId vertex){
        int size = cycleDetection.size();

        HashMap<TransactionId,Boolean> visited = new HashMap<>(size);

        return dfs(visited,vertex);
    }

    private Boolean dfs(Map<TransactionId,Boolean> visited,TransactionId vertex){
        if (visited.getOrDefault(vertex, false)){
            return true;
        }

        visited.put(vertex,true);

        Set<TransactionId> list = cycleDetection.get(vertex);
        if (list == null){
            return false;
        }

        for (TransactionId tid : list) {
            if (dfs(visited,tid)){
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return "DeadLock{" +
                "cycleDetection=" + cycleDetection +
                '}';
    }
}
