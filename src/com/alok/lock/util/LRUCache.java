package com.alok.lock.util;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * See: http://www.source-code.biz/snippets/java/6.htm
 * An LRU cache, based on <code>LinkedHashMap</code>.<br>
 * This cache has a fixed maximum number of elements (<code>cacheSize</code>).
 * If the cache is full and another entry is added, the LRU (least recently used) entry is dropped.
 * <p/>
 * This class is thread-safe. All methods of this class are synchronized.<br>
 * Author: Christian d'Heureuse (<a href="http://www.source-code.biz">www.source-code.biz</a>)<br>
 * License: <a href="http://www.gnu.org/licenses/lgpl.html">LGPL</a>.
 */
public class LRUCache<K, V> {

    private static final float hashTableLoadFactor = 0.75f;

    private LinkedHashMap<K, TimedValueWrapper<V>> map;
    private final int cacheSize;
    private long expireInSeconds;

    /**
     * Creates a new LRU cache.
     *
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     */

    public LRUCache(int cacheSize, long expireInSeconds) {
        this.cacheSize = cacheSize;
        int hashTableCapacity = (int) Math.ceil(cacheSize / hashTableLoadFactor) + 1;
        this.expireInSeconds = expireInSeconds;
        this.map = new LinkedHashMap<K, TimedValueWrapper<V>>(hashTableCapacity, hashTableLoadFactor, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, TimedValueWrapper<V>> eldest) {
                if (size() > LRUCache.this.cacheSize) {
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Retrieves an entry from the cache.<br>
     * The retrieved entry becomes the MRU (most recently used) entry.
     *
     * @param key the key whose associated value is to be returned.
     * @return the value associated to this key, or null if no value with this key exists in the cache.
     */
    public synchronized V get(K key) {
        TimedValueWrapper<V> value = map.get(key);
        V result = value == null ? null : (value.hasExpired() ? null : value.getValue());
        if(value != null && value.hasExpired()){
            remove(key);
        }
        return result;
    }

    public synchronized boolean containsKey(K key) {
        return map.get(key) != null && !map.get(key).hasExpired();
    }

    public synchronized void remove(K key){
        map.remove(key);
    }


    /**
     * Adds an entry to this cache.
     * The new entry becomes the MRU (most recently used) entry.
     * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
     * If the cache is full, the LRU (least recently used) entry is removed from the cache.
     *
     * @param key   the key with which the specified value is to be associated.
     * @param value a value to be associated with the specified key.
     */
    public synchronized void put(K key, V value) {
        map.put(key, new TimedValueWrapper<V>(expireInSeconds*1000, value));
    }

    /**
     * Clears the cache.
     */
    public synchronized void clear() {
        map.clear();
    }

    /**
     * Returns the number of used entries in the cache.
     *
     * @return the number of entries currently in the cache.
     */
    public synchronized int usedEntries() {
        return map.size();
    }

    public Set<K> keySet() {
        Set<K> keys = null;
        synchronized (this){
            keys = new HashSet<K>(map.keySet());
        }
        Set<K> validKeys = new HashSet<K>();
        for (K key : keys) {
            if(map.get(key) != null){
                validKeys.add(key);
            }
        }
        return validKeys;
    }

    private static class TimedValueWrapper<K>{
        private long expirationTime;
        private K value;

        public TimedValueWrapper(long validFor, K value){
            this.expirationTime = (validFor > 0 ? System.currentTimeMillis() + validFor : -1);
            this.value = value;
        }

        public boolean hasExpired(){
            return expirationTime > 0 && System.currentTimeMillis() > expirationTime;
        }

        public K getValue(){
            return value;
        }
    }
} // end class LRUCache