package simpledb.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class LRUCache<K,V> {

    private int size;

    private int capacity;

    private Map<Object, Node<K,V>> cache;

    private Node<K,V> head;

    private Node<K,V> tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        this.cache = new HashMap<>();
        this.head = new Node<>();
        this.tail = new Node<>();
        head.next = tail;
        tail.prev = head;
    }

    public synchronized V get(K key) {
        Node<K,V> node = this.cache.get(key);

        if (node == null) {
            return null;
        }
        moveToHead(node);
        return node.getValue();
    }

    public synchronized void put(K key, V value) {
        Node<K,V> node = cache.get(key);
        if (node == null) {
            node = new Node<>(key, value);
            cache.put(key, node);
            size++;
            addToHead(node);
//            if (size > capacity) {
//                Node<K,V> tail = removeTail();
//                cache.remove(tail.getKey());
//                size--;
//            }
        } else {
            node.setValue(value);
            moveToHead(node);
        }
    }

    public synchronized V remove(K key){
        Node<K,V> node = this.cache.get(key);
        if (node == null){
            return null;
        }
        removeNode(node);
        size--;
        this.cache.remove(key);
        return node.getValue();
    }

    public synchronized Node<K, V> getHeadNode(){
        return this.head;
    }

    public synchronized Node<K, V> getTailNode(){
        return this.tail;
    }

    public synchronized int size(){
        return size;
    }

    public synchronized Boolean isFull(){
        return size >= capacity;
    }


    private void addToHead(Node<K,V> node) {
        node.next = head.next;
        node.prev = head;
        node.next.prev = node;
        head.next = node;
    }

    private void removeNode(Node<K,V> node) {
        node.next.prev = node.prev;
        node.prev.next = node.next;
        node.prev = null;
        node.next = null;
    }

    private void moveToHead(Node<K,V> node) {
        removeNode(node);
        addToHead(node);
    }

    private Node<K,V> removeTail() {
        Node<K,V> node = this.tail.prev;
        removeNode(node);
        return node;
    }

}

class Node<K,V> {

    private K key;
    private V value;
    Node<K,V> prev;
    Node<K,V> next;

    public Node(){}

    public Node(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public synchronized Node<K,V> getNext(){
        return this.next;
    }

    public synchronized Boolean hasNext(){
        return this.next != null;
    }

    public synchronized Node<K,V> getPrev(){
        return this.prev;
    }

    public synchronized Boolean hasPrev(){
        return this.next != null;
    }

    public synchronized K getKey(){
        return this.key;
    }

    public synchronized V getValue(){
        return this.value;
    }

    public synchronized void setKey(K key){
        this.key = key;
    }

    public synchronized void setValue(V value){
        this.value = value;
    }

}