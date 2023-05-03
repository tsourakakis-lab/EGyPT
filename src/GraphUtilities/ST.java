// from Sedgewick https://introcs.cs.princeton.edu/java/code/javadoc/ST.html
package GraphUtilities;


import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

 
public class ST<Key extends Comparable<Key>, Value> implements Iterable<Key>{
    
    
     private TreeMap<Key, Value> st;
    //private HashMap<Key, Value> st;

    /**
     * Create an empty symbol table.
     */
    public ST() {
        st = new TreeMap<Key, Value>();
        //st = new HashMap<Key, Value>();
    }

    /**
     * Put key-value pair into the symbol table. Remove key from table if
     * value is null.
     */
    public void put(Key key, Value val) {
        if (val == null) st.remove(key);
        else             st.put(key, val);
    }

    /**
     * Return the value paired with given key; null if key is not in table.
     */
    public Value get(Key key) {
        return st.get(key);
    }

    /**
     * Delete the key (and paired value) from table.
     * Return the value paired with given key; null if key is not in table.
     */
    public Value delete(Key key) {
        return st.remove(key);
    }

    /**
     * Is the key in the table?
     */
    public boolean contains(Key key) {
        return st.containsKey(key);
    }

    /**
     * How many keys are in the table?
     */
    public int size() {
        return st.size();
    }

    /**
     * Return an <tt>Iterator</tt> for the keys in the table.
     * To iterate over all of the keys in the symbol table <tt>st</tt>, use the
     * foreach notation: <tt>for (Key key : st)</tt>.
     */ 
    public Iterator<Key> iterator() {
        return st.keySet().iterator();
    }


    /**
     * Return an <tt>Iterable</tt> for the keys in the table.
     * To iterate over all of the keys in the symbol table <tt>st</tt>, use the
     * foreach notation: <tt>for (Key key : st.keys())</tt>.
     */ 
    public Iterable<Key> keys() {
        return st.keySet();
    }


    public Key min() //Return the smallest key in the table.
    {
        return st.firstKey();
    }


    public Key max() //Return the largest key in the table.
    {
        return st.lastKey();
    }


    public Key ceil(Key k) //Return the smallest key in the table >= k.
    {
        SortedMap<Key, Value> tail = st.tailMap(k);
        if (tail.isEmpty()) return null;
        else return tail.firstKey();
    }

    public Key floor(Key k) //Return the largest key in the table <= k.
    {
        if (st.containsKey(k)) return k;

        // does not include key if present (!)
        SortedMap<Key, Value> head = st.headMap(k);
        if (head.isEmpty()) return null;
        else return head.lastKey();
    }
    
}

