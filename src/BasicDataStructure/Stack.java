/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BasicDataStructure;

/**
 *
 * @author tsourolampis
 */
public class Stack {
    
    private int N;         // number of elements on queue
    private Node first;    // top of stack

    // helper linked list class
    private class Node {
        private int data;
        private Node next;
    }

   /**
     * Create an empty stack.
     */
    public Stack() {
        first = null;
    }

   /**
     * Is the stack empty?
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
    public void push(int item) {
        Node old = first;
        first = new Node();
        first.data = item;
        first.next = old;
        N++;
    }

   /**
     * Remove and return the item on the queue least recently added.
     * Throw an exception if the queue is empty.
     */
    public int pop() {
        if (isEmpty()) return -1;// throw new RuntimeException("stack underflow");
        int item = first.data;
        first = first.next;
        N--;
        return item;
    }

    public void printS()
    {
        if( isEmpty() )
        {
            System.out.println("Stack is empty");
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
