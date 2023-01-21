package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;

public class DeadLock {
    private final Map<TransactionId, List<TransactionId>> cycleDetection;

    public DeadLock() {
        cycleDetection = new HashMap<>();
    }

    public void addEdge(TransactionId s,TransactionId d){
        List<TransactionId> list = cycleDetection.get(s);
        if (list == null){
            list = new LinkedList<>();
            list.add(d);
            cycleDetection.put(s,list);
            return;
        }

        list.add(d);
    }

    public void removeEdge(TransactionId s,TransactionId d){
        List<TransactionId> list = cycleDetection.get(s);
        if (list == null){
            return;
        }

        list.remove(d);
        if (list.size() == 0){
            cycleDetection.remove(s);
        }
    }

    public void removeVertex(TransactionId vertex){
        cycleDetection.remove(vertex);
        for (Map.Entry<TransactionId, List<TransactionId>> entry : cycleDetection.entrySet()) {
            entry.getValue().remove(vertex);
        }
    }

    public Boolean cycleDetection(TransactionId vertex){
        int size = cycleDetection.size();
        //System.out.println(cycleDetection);
        HashMap<TransactionId,Boolean> visited = new HashMap<>(size);

        return dfs(visited,vertex);
    }

    private Boolean dfs(Map<TransactionId,Boolean> visited,TransactionId vertex){
        if (visited.getOrDefault(vertex, false)){
            return true;
        }

        visited.put(vertex,true);

        List<TransactionId> list = cycleDetection.get(vertex);
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
