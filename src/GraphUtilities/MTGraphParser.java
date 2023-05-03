package GraphUtilities;

import GraphPartitioner.MTStreamingPartitioner;
import java.util.concurrent.BlockingQueue;
import java.util.ArrayList;
 
public class MTGraphParser implements Runnable{
    Thread thread;
    int threadId;
    Graph parent;
    BlockingQueue<ArrayList<String>> publicLinesQueue;
    int totalClassified = 0;

    public MTGraphParser(int id, Graph p, BlockingQueue<ArrayList<String>> plq)
    {
        thread = new Thread(this, "GraphWorkerThread_" + ((Integer)id).toString());
        threadId = id;
        parent = p;
        publicLinesQueue = plq;
        
        thread.start();
    }

    public void run()
    {
        boolean Exit = false;
        
        while(!Exit)
        {
            try 
            {
                ArrayList<String> lines;
                lines = publicLinesQueue.take();
                for(String line : lines)
                {
                    parent.MTParseLine(line);
                    totalClassified ++;
                }
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
