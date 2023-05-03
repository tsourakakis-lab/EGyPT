package GraphPartitioner;

import GraphUtilities.Graph;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
 
public class MTStreamingPartitioner extends StreamingPartitioner {
    private int degS[];
    private BlockingQueue<Integer> publicVerticesQueue;
    private int numberOfThreads;
    private ArrayList<MTWorkerThread> Sites;

    //Copied from the serial version 
    public Integer[] MTloadNN;
    public Double MTfcutNN;
    private double MTmaxloadNN=0.0;

    

    public MTStreamingPartitioner(Graph G, int k)
    {
        this(G, k, 1);
    }

    
    public MTStreamingPartitioner(Graph G, int k, int noThreads)
    {
        super(G, k);
        degS =  new int[clusters];
        numberOfThreads = noThreads;
        MTfcutNN = 0.0;
        
        publicVerticesQueue = new LinkedBlockingQueue();
        Sites = new ArrayList();
    }

    
    protected int MTNonNeighborsPartitioning_GetWinner(int v)
    {
        int counter = 0;
        double min = Double.MAX_VALUE;
        int winner = 0;
        ArrayList al = new ArrayList();
        for( HashSet S:  vsets)
        {
            degS[counter]=G.InducedDegree_FAST(S, v);
            double score = MTloadNN[counter] - degS[counter];
            if( score <= min ){
                if ( Math.abs(min-score) <= ERR )
                {
                    al.add(counter);
                    min = score;
                }
                else
                {
                    al.clear();
                    al.add(counter);
                    min=score;
                }
            }
            counter++;
        }
        if( al.size() > 1)
        {

            Random rr = new Random();
            int w2 = rr.nextInt(al.size());
            counter = 0;
            int counter2=0;
            for( HashSet S:  vsets)
            {
                if( al.contains((Integer)counter))
                {
                    if( w2== counter2 )
                        winner = counter;
                    counter2++;
                }
                counter++;
            }
        }
        else
        {
            winner = (Integer) al.remove(0);
        }

        return winner;
    }

    
    protected void MTNonNeighborsPartitioning_PlaceWinner_wLocks(int v, int winner)
    {
        // System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
        synchronized(partitionmap)
        {
            partitionmap.put(v, winner);
        }
        
        int counter = 0;
        for( HashSet S:  vsets)
        {

            if( counter == winner )
            {
                synchronized(S)
                {
                    S.add(v);
                }
                synchronized(MTloadNN[counter])
                {
                    MTloadNN[counter] +=1;
                }
            }
            else
            {
                synchronized(MTfcutNN)
                {
                    MTfcutNN += degS[counter];
                }
            }
            counter++;
        }
    }
    
    
    
    protected void MTNonNeighborsPartitioning_PlaceWinner(int v, int winner)
    {
        // System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
        partitionmap.put(v, winner);
        int counter = 0;
        for( HashSet S:  vsets)
        {

            if( counter == winner )
            {
                S.add(v);
                MTloadNN[counter] +=1;
            }
            else
            {
                MTfcutNN += degS[counter];
            }
            counter++;
        }
    }
    
    public void MTNonNeighborsPartitioning_ClearStructures()
    {
        MTloadNN = new Integer[clusters];
        Arrays.fill(MTloadNN, 0);
        vsets.clear();
        HashSet hs;
        MTfcutNN = 0.0;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }    
    }

    
    public void NonNeighborsPartitioning()
    {
        int v;
        MTNonNeighborsPartitioning_ClearStructures();
                
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            int counter = 0;
            int winner; 

            v = order[i];
            //   System.out.println("Vertex "+ v+" arrived");

            winner = MTNonNeighborsPartitioning_GetWinner(v);
            
            MTNonNeighborsPartitioning_PlaceWinner(v, winner);
            
        }
        long endTime = System.currentTimeMillis();
        timeNN = (endTime - startTime);
        MTfcutNN = MTfcutNN/m;
        MTmaxloadNN = (double)findMax(MTloadNN)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Nonneighbors partitioning");
        printLoads(MTloadNN);
        System.out.println("Timing nonneighbors: "+timeNN+" milliseconds");
        System.out.println("Fraction of edges cut "+MTfcutNN);
        System.out.println("Max load (normalized) "+ MTmaxloadNN );
    }

    public void MTNonNeighborsPartitioning()
    {
        int v;
        MTNonNeighborsPartitioning_ClearStructures();
        
        for(int i=0; i<numberOfThreads; i++)
        {
            MTWorkerThread worker;
            worker = new MTWorkerThread(i, G, this, clusters, ERR, publicVerticesQueue); 

            Sites.add(worker);
        }

        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];
            publicVerticesQueue.add(v); 
        }

        // Wait until the queue is empty
        try
        {
            while (!publicVerticesQueue.isEmpty())
            {
                Thread.currentThread().sleep(10);
                //System.out.format("Remaining %d vertices%n", publicVerticesQueue.size());
            }
            Thread.currentThread().sleep(10);

            System.out.print("Thread stats: ");
            for(MTWorkerThread t : Sites)
            {
                t.finish();
            }
            System.out.println("");
        
        }
        catch (InterruptedException e)
        {
        }
        
        
        long endTime = System.currentTimeMillis();
        timeNN = (endTime - startTime);
        MTfcutNN = MTfcutNN/m;
        MTmaxloadNN = (double)findMax(MTloadNN)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Nonneighbors partitioning");
        printLoads(MTloadNN);
        System.out.println("Timing nonneighbors: "+timeNN+" milliseconds");
        System.out.println("Fraction of edges cut "+MTfcutNN);
        System.out.println("Max load (normalized) "+ MTmaxloadNN );
    }

    
    
    /** Returns the maximum value from an array v */
    private Integer findMax(Integer[] v)
    {
        //   System.out.println("length of v "+v.length);
        Integer max = v[0];
        //Integer ind = 0;
        for(Integer i =1;i< v.length;i++)
        {
            if( max<v[i])
            {
                max = v[i];
                // ind = i;
            }
        }
        return max;
    }
    
    
    /** Returns the minimum value from an array v */
    private Integer findMin(Integer[] v)
    {
        Integer min = v[0];
        //Integer ind = 0;
        for(Integer i =1;i< v.length;i++)
        {
            if( min>v[i])
            {
                min = v[i];
                // ind = i;
            }
        }
        return min;
    }
    
    private void printLoads(Integer[] v)
    {
        
        // System.out.println("**LOADS**");
        System.out.print("  |");
        for(Integer i=0; i<v.length; i++)
        {
            System.out.print(v[i]+"|");
        }
        System.out.println("");
        // System.out.println("*********");
    }

    
}
