package GraphUtilities.StreamOrder;

import GraphUtilities.Graph;
import java.util.Random;

public class RandomOrder {
    
    
    public int[] randomorder; //n+1 array of integers 
    private int n;
    
    // initializes the randomorder with identity permutation 
    public RandomOrder(Graph G)
    {
        randomorder = new int[G.V()+1]; 
        n = G.V();
        randomorder[0]=-10; //garbage  
        for(int i=1; i<=G.V(); i++)
                    randomorder[i]=i;
        
    }
    
    public void getRandomOrder()
    {
        int j;
        Random randomGenerator = new Random();
        for(int i=n; i>=1;i-- )
        {
            j = randomGenerator.nextInt(i)+1;
            int swap = randomorder[i];
            randomorder[i] =  randomorder[j];
            randomorder[j] = swap; 
        }
        return;
    }
    
    
    public void printOrder()
    {
        System.out.print("[");
        for(int i = 1; i<= n-1; i++)
            System.out.print(randomorder[i]+",");
        System.out.println(randomorder[n]+"]");
    }
    
    
    
}
