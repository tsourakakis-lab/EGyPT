package GraphUtilities;
 
import java.util.TreeSet;
import java.util.Iterator;
import java.util.SortedSet;



/**
 *  The <tt>SET</tt> class represents an ordered set. It assumes that
 *  the elements are <tt>Comparable</tt>.
 *  It supports the usual <em>add</em>, <em>contains</em>, and <em>delete</em>
 *  methods. It also provides ordered methods for finding the <em>minimum</em>,
 *  <em>maximum</em>, <em>floor</em>, and <em>ceiling</em>.
 *  <p>
 *  This implementation uses a balanced binary search tree.
 *  The <em>add</em>, <em>contains</em>, <em>delete</em>, <em>minimum</em>,
 *  <em>maximum</em>, <em>ceiling</em>, and <em>floor</em> methods take
 *  logarithmic time.
 *  <p>
 *  For additional documentation, see <a href="http://introcs.cs.princeton.edu/44st">Section 4.4</a> of
 *  <i>Introduction to Programming in Java: An Interdisciplinary Approach</i> by Robert Sedgewick and Kevin Wayne.
 *  */

public class SET<Key extends Comparable<Key>> implements Iterable<Key> {
    private TreeSet<Key> set;

    /**
     * Create an empty set.
     */
    public SET() {
        set = new TreeSet<Key>();
    }

    /**
     * Is this set empty?
     */
    public boolean isEmpty() {
        return set.isEmpty();
    }
 
    /**
     * Add the key to this set.
     */
    public void add(Key key) {
        set.add(key);
    }

    /**
     * Does this set contain the given key?
     */
    public boolean contains(Key key) {
        return set.contains(key);
    }

    /**
     * Delete the given key from this set.
     */
    public void delete(Key key) {
        set.remove(key);
    }

    /**
     * Return the number of keys in this set.
     */
    public int size() {
        return set.size();
    }

    /**
     * Return an Iterator for this set.
     */
    public Iterator<Key> iterator() {
        return set.iterator();
    }

    /**
     * Return the key in this set with the maximum value.
     */
    public Key max() {
        return set.last();
    }

    /**
     * Return the key in this set with the minimum value.
     */
    public Key min() {
        return set.first();
    }

    /**
     * Return the smallest key in this set >= k.
     */
    public Key ceil(Key k) {
        SortedSet<Key> tail = set.tailSet(k);
        if (tail.isEmpty()) return null;
        else return tail.first();
    }

    /**
     * Return the largest key in this set <= k.
     */
    public Key floor(Key k) {
        if (set.contains(k)) return k;

        // does not include key if present (!)
        SortedSet<Key> head = set.headSet(k);
        if (head.isEmpty()) return null;
        else return head.last();
    }

    /**
     * Return the union of this set with that set.
     */
    public SET<Key> union(SET<Key> that) {
        SET<Key> c = new SET<Key>();
        for (Key x : this) { c.add(x); }
        for (Key x : that) { c.add(x); }
        return c;
    }

    /**
     * Return the intersection of this set with that set.
     */
    public SET<Key> intersects(SET<Key> that) {
        SET<Key> c = new SET<Key>();
        if (this.size() < that.size()) {
            for (Key x : this) {
                if (that.contains(x)) c.add(x);
            }
        }
        else {
            for (Key x : that) {
                if (this.contains(x)) c.add(x);
            }
        }
        return c;
    }
}