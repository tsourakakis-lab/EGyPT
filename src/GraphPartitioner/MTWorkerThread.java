package GraphPartitioner;

import GraphUtilities.Graph;
import java.util.*;
import java.util.concurrent.*;

public class MTWorkerThread implements Runnable{
    Thread thread;
    int threadId;
    int noClusters;
    private MTStreamingPartitioner parent;
    private int degS[];
    private BlockingQueue<Integer> publicVerticesQueue;
    private boolean Exit = false;
    private double ERR;
    private Graph G;
    private int totalClassified;
    


  
            
    public MTWorkerThread(int id, Graph graph, MTStreamingPartitioner p, 
            int clusters, double numERR, 
            BlockingQueue<Integer> publicQueue)
    {
        thread = new Thread(this, "WorkerThread_" + ((Integer)id).toString());
        threadId = id;
        parent = p;
        noClusters = clusters;
        degS =  new int[noClusters];
        ERR = numERR;
        G = graph;
        publicVerticesQueue = publicQueue;
        totalClassified = 0;
        
        thread.start();
    }
    
    
    private void ProcessVertex(int v)
    {
        int winner;
        winner = parent.MTNonNeighborsPartitioning_GetWinner(v);
        parent.MTNonNeighborsPartitioning_PlaceWinner_wLocks(v, winner);    
        totalClassified++;
    }
    
    
    public void run()
    {
        
        while(!Exit)
        {
            try 
            {
                int vertex;
                vertex = publicVerticesQueue.take();
                ProcessVertex(vertex);
            }
            catch (InterruptedException e) {
                Exit = true;
            }

        }
    }

    public void finish()
    {
        //System.out.format("Thread %d, classified %d vertices%n", threadId, totalClassified);
        System.out.format("(%d,%d) ", threadId, totalClassified);
        // Thread is typically blocked waiting on the BlockingQueue
        // so we need to force it to exit
        thread.interrupt();
    }
}

