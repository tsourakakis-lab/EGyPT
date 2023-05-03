package GraphUtilities.StreamOrder;

import GraphUtilities.Graph;
import java.util.Arrays;

public class DepthFirstSearch {
    
    
    public int[] dfsorder; 
    private boolean[] marked; 
    public int counter; 
    private int n; 
    
    public DepthFirstSearch(Graph G)
    {
        n = G.V();
        dfsorder = new int[G.V()+1]; 
        marked = new boolean[G.V()+1];
        Arrays.fill(marked, Boolean.FALSE);
        counter = 0; 
    }
    
    
 
        
    public void dfs(Graph G, int v)
    {
        marked[v]=true; 
        counter++;
        dfsorder[counter]=v;
        for( int w : G.adjacentTo(v))
        {
            if(!marked[w]){
                marked[w]=true;
                dfs(G,w);
            }   
        }
    }
    
    public void getDFSorder(Graph G)
    {
        RandomOrder RO = new RandomOrder(G);
        RO.getRandomOrder();
        int[] randomorder = RO.randomorder;
        
        for( int i = 1; i <= G.V(); i++)
            if( !marked[randomorder[i]] ) 
                 dfs(G,randomorder[i]);
        
    }
    
          public void printOrder()
    {
        System.out.print("[");
        for(int i = 1; i<= n-1; i++)
            System.out.print(dfsorder[i]+",");
        System.out.println(dfsorder[n]+"]");
    }
}
