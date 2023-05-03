
import GraphPartitioner.MTStreamingPartitioner;
import GraphPartitioner.StreamingPartitioner;
import GraphUtilities.Graph;
import GraphUtilities.PointerGraph;
import GraphUtilities.ArrayGraph;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
 
public class MTMain {
    public static Integer runningThreads = 0;

  
    
    public static void main(String[] args) throws IOException {

        CmdArgs Arguments = new CmdArgs(args);

        System.out.print("Clustering " + Arguments.fileName + " into (");
        for(int i=0; i<Arguments.noClusters.length; i++)
        {
            System.out.print(" " + Arguments.noClusters[i]);
        }
        System.out.print(" ) clusters with algorithm types (");
        for(int i=0; i<Arguments.Types.length; i++)
        {
            System.out.print(" " + Arguments.Types[i]);
        }
        System.out.println(" )");
        
        if (Arguments.createGiraphFiles)
        {
            System.out.println("With dumping Giraph files");
        }
        
        double starttime;
        double total;

        
        
        starttime = System.currentTimeMillis();
        Graph G =  new ArrayGraph(Arguments.fileName,"\t", 0, Arguments.useHeader);
        //Graph G =  new PointerGraph(filename,"\t", noThreads);
        total = System.currentTimeMillis()-starttime;
        System.out.println("total time to read the graph "+ total + " ms");
        
        System.out.format("Number of vertices %d, edges %d%n", G.V(), G.E());

        /*
        if (noClusters > 0)
        {
            RunOne(G, noClusters, filename, createGiraphFiles);
        }
        else
        */
        {
            RunAll(G, Arguments.noClusters, Arguments.Types, Arguments.createGiraphFiles);
            //ExportToMetis(G);
        }
        
        System.gc(); //garbage collector immediately
        
        
    }
    
    
    
    public static void RunAll(Graph G, int [] NoClusters, int [] Types, boolean createGiraphFiles) throws IOException {
        
        String newfilename = "log_"+G.GetFilename()+"."+Math.random();
        String path = new java.io.File(".").getCanonicalPath();
        File f = new File(path, newfilename);
        f.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));


        bw.write("% Legend: ");
        for (int nt=0; nt<Types.length; nt++)
        {
            bw.write(" " + Types[nt] + "-" + MTMainWorker.getAlgoName(Types[nt])); 
        }
        bw.write("\n");

        
        System.out.println("Running " + NoClusters.length + " #clusters with " + Types.length + " #types in parallel...");
        
        runningThreads = NoClusters.length * Types.length;
        MTMainWorker[] workers = new MTMainWorker[runningThreads];
        
        for (int nc = 0; nc < NoClusters.length; nc++)
        {
            for (int nt=0; nt<Types.length; nt++)
            {
                workers[nc*Types.length+nt] = new MTMainWorker(1, G, NoClusters[nc], Types[nt], bw, newfilename, createGiraphFiles); 
            }
        }
        
        try
        {
            int tmpcnt = 0;
            while(runningThreads > 0)
            {
                Thread.currentThread().sleep(1000);
                if (tmpcnt == 60)
                {
                    System.out.println(G.GetFilename() + " remaining threads: " + runningThreads);
                    tmpcnt = 0;
                }
                tmpcnt++;
            }
        }
        catch (InterruptedException e)
        {
        }
        
        bw.close();
        System.out.println("Finished");
    }

    
    protected static void DecreaseRunningThreads()
    {
        synchronized(runningThreads)
        {
            runningThreads--;
        }
    }
    
    
    
    
    public static void RunOne(Graph G, int noClusters, String filename, boolean createGiraphFiles)  throws IOException 
    {
        double starttime;
        double total;
        String outFileName;
        

        StreamingPartitioner sp = new StreamingPartitioner(G,noClusters);
        
        
        // No sorting, use the default order
        /*
        starttime = System.currentTimeMillis();
        System.out.println("Sorting...");
        sp.setStreamOrder("BFS");
        total = System.currentTimeMillis()-starttime;
        System.out.println("Sorted, total time " + total);
        //sp.setStreamOrder("Random");
        */

        starttime = System.currentTimeMillis();
        //sp.NonNeighborsPartitioning();
        //sp.NonNeighborsConvexf(1.5);
        sp.NonNeighborsConvexfKnee(1.25, 1.5);
        System.out.println("***********************************************");
        total = System.currentTimeMillis()-starttime;
        System.out.println("total elapsed time "+ total);
        if (createGiraphFiles)
        {
            System.out.println("Dumping out the partition...");
            outFileName = filename.replace(".txt", "");
            sp.Partition2GiraphClustered(outFileName + "_mFennel", "txt");
        }
        

        starttime = System.currentTimeMillis();
        //sp.NonNeighborsPartitioning();
        sp.LinearWeightedDegreePartitioning();
        System.out.println("***********************************************");
        total = System.currentTimeMillis()-starttime;
        System.out.println("total elapsed time "+ total);
        if (createGiraphFiles)
        {
            System.out.println("Dumping out the partition...");
            outFileName = filename.replace(".txt", "");
            sp.Partition2GiraphClustered(outFileName + "_mStanton", "txt");
        }

        
        starttime = System.currentTimeMillis();
        sp.HashingPartitioning();
        System.out.println("***********************************************");
        total = System.currentTimeMillis()-starttime;
        System.out.println("total elapsed time "+ total);
        if (createGiraphFiles)
        {
            System.out.println("Dumping out the partition...");
            outFileName = filename.replace(".txt", "");
            sp.Partition2GiraphClustered(outFileName + "_mRandom", "txt");
        }
        
        /*
        sp.setStreamOrder("BFS");
        sp.Partition2GiraphOblivious(outFileName + "_mBFS", "txt");
        sp.setStreamOrder("Random");
        sp.Partition2GiraphOblivious(outFileName + "_mRandom", "txt");
        */

        /*
        starttime = System.currentTimeMillis();        
        sp.MTNonNeighborsPartitioning();
        System.out.println("***********************************************");
        total = System.currentTimeMillis()-starttime;
        System.out.println("total elapsed time "+ total);
        */
        
    }

        
        
        
    public static void ExportToMetis(Graph G) throws IOException 
    {
        int v;
        int vertexCnt = 0, vertexStep;
        int [] order = G.defaultOrder();

        
        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        

        String newfilename = "METIS"+ G.GetFilename()+"."+Math.random();
        String path = new java.io.File(".").getCanonicalPath();
        File f = new File(path, newfilename);
        f.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        
        // Header line
        bw.write(G.V() + " " + G.E() + "\n");
        
        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("NonNeighborsConvexfKnee(" + G.GetFilename() + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }
            
            for(int j: G.adjacentTo(v))
            {
                bw.write(j+" ");
            }
            bw.write("\n");
            
        }
        bw.close();
            
        long endTime = System.currentTimeMillis();
        System.out.println("Timing export time: "+(endTime - startTime) +" milliseconds");
    }
        
}
