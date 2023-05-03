package GraphUtilities.StreamOrder;

import BasicDataStructure.Queue;
import GraphUtilities.Graph;
import GraphUtilities.ST;
import java.util.Arrays;
 
public class BreadthFirstSearch {
    
    private ST<Integer, Integer>  prev = new ST<Integer, Integer>();
    private ST<Integer, Integer> dist = new ST<Integer, Integer>();
    public int[] bfsorder;
    private boolean[] marked;
    private int counter;
    private int  n; 
    
    public BreadthFirstSearch(Graph G)
    {
        n = G.V();
        bfsorder = new int[n+1];
        marked = new boolean[n+1];
        Arrays.fill(marked, Boolean.FALSE);
        counter = 0;
    }
    
    
    
    public void getBFSorder(Graph G)
    {
        RandomOrder RO = new RandomOrder(G);
        RO.getRandomOrder();
        int[] randomorder = RO.randomorder;
        
        System.out.println("number of vertices "+n+" "+G.V());
        for( int i = 1; i <= n; i++)
            if( !marked[randomorder[i]] )
            {
                bfs(G,randomorder[i]);
            }
        
    }
    
   
    
    // run BFS in graph G from given source vertex s
    public void bfs(Graph G, int s) {
        
        marked[s] = true;
        bfsorder[++counter]=s; 
       //  System.out.println("counter = "+counter);
        Queue q = new Queue();
        q.enqueue(s);
        dist.put(s, 0);
        while (!q.isEmpty()) {
            int v = q.dequeue();
            for (Integer w : G.adjacentTo(v)) {
                if (!dist.contains(w)) {
                    q.enqueue(w);
                    bfsorder[++counter]=w;
                    marked[w]=true;
                    //System.out.println("counter = "+counter);
                    dist.put(w, 1 + dist.get(v));
                    prev.put(w, v);
                }
            }
        }
    }
    

    public void printOrder()
    {
        System.out.print("[");
        for(int i = 1; i<= n-1; i++)
            System.out.print(bfsorder[i]+",");
        System.out.println(bfsorder[n]+"]");
    }
    

}
