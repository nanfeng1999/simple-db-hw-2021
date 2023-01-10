package simpledb.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class LRUCache<K,V> {

    private int size;

    private int capacity;

    private Map<Object, DoubleLinkedNode<K,V>> cache;

    private DoubleLinkedNode<K,V> head;

    private DoubleLinkedNode<K,V> tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        this.cache = new HashMap<>();
        this.head = new DoubleLinkedNode<>();
        this.tail = new DoubleLinkedNode<>();
        head.next = tail;
        tail.prev = head;
    }

    public V get(K key) {
        DoubleLinkedNode<K,V> node = this.cache.get(key);

        if (node == null) {
            return null;
        }
        moveToHead(node);
        return node.value;
    }

    public void put(K key, V value) {
        DoubleLinkedNode<K,V> node = cache.get(key);
        if (node == null) {
            node = new DoubleLinkedNode<>(key, value);
            cache.put(key, node);
            size++;
            addToHead(node);
            if (size > capacity) {
                DoubleLinkedNode<K,V> tail = removeTail();
                cache.remove(tail.key);
                size--;
            }
        } else {
            node.value = value;
            moveToHead(node);
        }
    }

    public V removeOldest(){
        DoubleLinkedNode<K,V> tail = removeTail();
        return tail.value;
    }

    public V remove(K key){
        DoubleLinkedNode<K,V> node = this.cache.get(key);
        if (node == null){
            return null;
        }
        removeNode(node);
        return node.value;
    }

    public Set<Map.Entry<Object, DoubleLinkedNode<K, V>>> entrySet(){
        return this.cache.entrySet();
    }

    public int size(){
        return size;
    }

    static class DoubleLinkedNode<K,V> {

        K key;

        V value;

        DoubleLinkedNode<K,V> prev;

        DoubleLinkedNode<K,V> next;

        public DoubleLinkedNode() {}
        public DoubleLinkedNode(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private void addToHead(DoubleLinkedNode<K,V> node) {
        node.next = head.next;
        node.prev = head;
        node.next.prev = node;
        head.next = node;
    }

    private void removeNode(DoubleLinkedNode<K,V> node) {
        node.next.prev = node.prev;
        node.prev.next = node.next;
        node.prev = null;
        node.next = null;
    }

    private void moveToHead(DoubleLinkedNode<K,V> node) {
        removeNode(node);
        addToHead(node);
    }

    private DoubleLinkedNode<K,V> removeTail() {
        DoubleLinkedNode<K,V> node = this.tail.prev;
        removeNode(node);
        return node;
    }

}