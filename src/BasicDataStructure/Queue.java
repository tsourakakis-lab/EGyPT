/*
 * Charalampos Tsourakakis
 */
package BasicDataStructure;


public class Queue {
    
    private int N;         // number of elements on queue
    private Node first;    // beginning of queue
    private Node last;     // end of queue

    // helper linked list class
    private class Node {
        private int data;
        private Node next;
    }

   /**
     * Create an empty queue.
     */
    public Queue() {
        first = null;
        last  = null;
    }

   /**
     * Is the queue empty?
     */
    public boolean isEmpty() {
        return first == null;
    }

   /**
     * Return the number of items in the queue.
     */
    public int size() {
        return N;     
    }


   /**
     * Return the item least recently added to the queue.
     * Throw an exception if the queue is empty.
     */
    public int peek() {
        if (isEmpty()) throw new RuntimeException("Queue underflow");
        return first.data;
    }

   /**
     * Add the item to the queue.
     */
    public void enqueue(int item) {
        Node x = new Node();
        x.data = item;
        if (isEmpty()) { first = x;     last = x; }
        else           { last.next = x; last = x; }
        N++;
    }

   /**
     * Remove and return the item on the queue least recently added.
     * Throw an exception if the queue is empty.
     */
    public int dequeue() {
        if (isEmpty()) return -1;// throw new RuntimeException("Queue underflow");
        int item = first.data;
        first = first.next;
        N--;
        if (isEmpty()) last = null;   // to avoid loitering
        return item;
    }

    public void printQ()
    {
        if( isEmpty() )
        {
            System.out.println("Queue is empty");
            return; 
        }
        Node current = first; 
        for(int i = 0;i<size();i++)
        {
            System.out.print(current.data+" ");
            current = current.next;
        }
        System.out.println("");
    }
   

  


}
