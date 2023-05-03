package GraphPartitioner;


import GraphUtilities.Graph;
import GraphUtilities.StreamOrder.BreadthFirstSearch;
import GraphUtilities.StreamOrder.DepthFirstSearch;
import GraphUtilities.StreamOrder.RandomOrder;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class StreamingPartitioner {

    // Sset to false to for debugging
    public static boolean useRandomness = true;
    
    public static final double EXP =  2.7183;
    public static final double ERR =  0.001; //numerical error
    
    /* Graph */
    public Graph G;
    int n;              // Number of vertices
    long m;             // Number of edges
    /* The number of clusters*/
    public int clusters;
    
    //public String[] stream={"BFS","DFS","Random"};
    //public String[] method={"N", "NLW", "NEW", "H", "B", "T", "TLW", "TEW","NN", "NNalpha", "NNf"};
    
    /* partition is a collection of hashsets */
    protected ArrayList<HashSet<Integer>> vsets;
    protected HashMap<Integer,Integer> partitionmap;
    

    
    
    public int[] order;
    public String sorder;
    //public double[] fcut;
    //public double[] maxload;
    // public String order2;
    
    
    /* Neighbors method, STANTON */
    public double fcutN=0;
    public int[] loadN;
    public double maxloadN=0.0;
    public double timeN = 0.0;
    /* Linear weighted Neighbors method */
    public double fcutLN=0.0;
    public int[] loadLN;
    public double maxloadLN=0.0;
    public double timeLN = 0.0;
    /* Linear weighted Neighbors method normalizing edges*/
    public double fcutLNE=0.0;
    public int[] loadLNE;
    public int[] eLoadLNE;
    public double maxloadLNE=0.0;
    public double timeLNE = 0.0;
    /* Exponential Weighted Neighbors method */
    public double fcutEWN=0;
    public int[] loadEWN;
    public double maxloadEWN=0.0;
    public double timeEWN = 0.0;
    /* Hashing method */
    public double fcutH=0;
    public int[] loadH;
    public int[] eLoadH;
    public double maxloadH=0.0;
    public double timeH= 0.0;
    /* Balanced method */
    public double fcutB=0;
    public int[] loadB;
    public double maxloadB=0.0;
    public double timeB = 0.0;
    /* Triangles */
    public double fcutT=0;
    public int[] loadT;
    public double maxloadT=0.0;
    public double timeT = 0.0;
    /*  Linear weighted triangles */
    public double fcutLT=0;
    public int[] loadLT;
    public double maxloadLT=0.0;
    public double timeLT = 0.0;
    /* exponential weighted triangles */
    public double fcutEWT=0;
    public int[] loadEWT;
    public double maxloadEWT=0.0;
    public double timeEWT = 0.0;
    /* Non Neighbors method*/
    // moved to private by bozidar
    private double fcutNN=0;
    private int[] loadNN;
    private double maxloadNN=0.0;
    public double timeNN = 0.0;
    /* Non Neighbors with alpha, STANTON */
    public double fcutNalpha=0;
    public int[] loadNalpha;
    public double maxloadNalpha=0.0;
    public double timeNalpha = 0.0;
    /* Convex f */
    public double fcutNf=0;
    public int[] loadNf;
    public double maxloadNf=0.0;
    public double timeNf = 0.0;
    /* Convex f with given normalization */
    public double fcutNfN=0;
    public int[] loadNfN;
    public double maxloadNfN=0.0;
    public double timeNfN = 0.0;
    /* Convex f with given knee */
    public double fcutNfKnee=0;
    public int[] loadNfKnee;
    public double maxloadNfKnee=0.0;
    public double timeNfKnee = 0.0;
    /* Convex f, normalized, with given knee */
    public double fcutNfNKnee=0;
    public int[] loadNfNKnee;
    public double maxloadNfNKnee=0.0;
    public double timeNfNKnee = 0.0;
    /* Convex f, normalizing edges, with given knee */
    public double fcutNfNKneeE=0;
    public int[] loadNfNKneeE;
    public int[] eLoadNfNKneeE;
    public double maxloadNfNKneeE=0.0;
    public double timeNfNKneeE = 0.0;
    /* Convex f with given knee */
    public double fcutNfKneeE=0;
    public int[] loadNfKneeE;
    public int[] eLoadNfKneeE;
    public double maxloadNfKneeE=0.0;
    public double timeNfKneeE = 0.0;
    
    
    
    /* Constructor */
    
    
    public void printStatistics(String filename, double alpha, double gamma) throws IOException
    {
        String newfilename = "Statistics"+sorder+"_Clusters"+clusters+"_"+filename+"."+Math.random();
        String path = new java.io.File(".").getCanonicalPath();
        File f = new File(path, newfilename);
        f.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        bw.write("Method\tTime\tFraction of edges cut\tMax load (normalized)\n");
        bw.write("Degree Partitioning\t"+timeN+"\t"+fcutN+"\t"+maxloadN+"\n");
        bw.write("Linear Weighted Degree Partitioning\t"+timeLN+"\t"+fcutLN+"\t"+maxloadLN+"\n");
        bw.write("Exponentially Weighted Degree Partitioning\t"+timeEWN+"\t"+fcutEWN+"\t"+maxloadEWN+"\n");
        bw.write("Hashing Partitioning\t"+timeH+"\t"+fcutH+"\t"+maxloadH+"\n");
        bw.write("Balanced Partitioning\t"+timeB+"\t"+fcutB+"\t"+maxloadB+"\n");
        bw.write("Triangle Partitioning\t"+timeT+"\t"+fcutT+"\t"+maxloadT+"\n");
        bw.write("Linearly Weighted Triangle Partitioning\t"+timeLT+"\t"+fcutLT+"\t"+maxloadLT+"\n");
        bw.write("Exponentially Weighted Triangle Partitioning\t"+timeEWT+"\t"+fcutEWT+"\t"+maxloadEWT+"\n");
        bw.write("Nonneighbors Partitioning\t"+timeNN+"\t"+fcutNN+"\t"+maxloadNN+"\n");
        bw.write("Quadratic Partitioning, alpha="+alpha+"\t"+timeNalpha+"\t"+fcutNalpha+"\t"+maxloadNalpha+"\n");
        bw.write("Convex Partitioning, gamma="+gamma+"\t"+timeNf+"\t"+fcutNf+"\t"+maxloadNf+"\n");
        bw.close();
    }
    

    public void printLineStatistics(BufferedWriter bw, int algoType, int displayType) throws IOException
    {
        synchronized(bw)
        {
            switch(displayType)
            {
                case 1:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeH +"\t"+fcutH+"\t"+maxloadH+"\n");
                    break;
                case 2:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeLN+"\t"+fcutLN+"\t"+maxloadLN+"\n");
                    break;
                case 3:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeNf+"\t"+fcutNf+"\t"+maxloadNf+"\n");
                    break;
                case 4:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeNfN+"\t"+fcutNfN+"\t"+maxloadNfN+"\n");
                    break;
                case 5:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeNfKnee+"\t"+fcutNfKnee+"\t"+maxloadNfKnee+"\n");
                    break;
                case 6:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeNfNKnee+"\t"+fcutNfNKnee+"\t"+maxloadNfNKnee+"\n");
                    break;
                case 7:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeNfNKneeE+"\t"+fcutNfNKneeE+"\t"+maxloadNfNKneeE+"\n");
                    break;
                case 8:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeNfKneeE+"\t"+fcutNfKneeE+"\t"+maxloadNfKneeE+"\n");
                    break;
                case 9:
                    bw.write(G.GetFilename() + "\t" + clusters + "\t" + algoType + "\t" + timeLNE+"\t"+fcutLNE+"\t"+maxloadLNE+"\n");
                    break;
            }
            bw.flush();
        }
    }
    
    
     /** This function prints the graph in a metis style format but with the following
     * difference. Each line contains the vertex id (first number in each line), its
     * assigned cluster (second number in each line, ranging from 0 to k-1) and then
     * the neighbourhood of that specific vertex.
     *
     */
    public void Partition2Christos(String filename) throws IOException
    {
        String newfilename = "ChristosPartition"+sorder+"_Clusters"+clusters+"_"+filename+"."+Math.random();
        String path = new java.io.File(".").getCanonicalPath();
        File f = new File(path, newfilename);
        f.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        Iterator it = partitionmap.keySet().iterator();
        while( it.hasNext() )
        {
            int key = (Integer)it.next();
            int value = (Integer) partitionmap.get(key);
            bw.write(key+","+value);
            for(int i: G.adjacentTo(key))
            {
                value = (Integer) partitionmap.get(i);
                bw.write(","+i+","+value);
            }
            bw.write("\n");
        }
        bw.close();
    }
    
    

     /** 
      * This function prints the graph in the Giraph format
     */
    public void Partition2GiraphClustered(String filename, String extension) throws IOException
    {
        String newfilename;
        String path = new java.io.File(".").getCanonicalPath();

        // Dump the graph in the original order
        for (int c=0; c < clusters; c++)
        {
            newfilename = filename+"_Giraph_c"+clusters+"_"+c+ "." + extension;
            System.out.println("Dumping: " + newfilename);
            File f = new File(path, newfilename);
            f.createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));
            
            HashSet v = vsets.get(c);

            Iterator it = v.iterator();
            while( it.hasNext() )
            {
                int key = (Integer)it.next();
                bw.write(key+","+c);
                for(int i: G.adjacentTo(key))
                {
                    int value;
                    value = (Integer) partitionmap.get(i);
                    bw.write(","+i+","+value);
                }
                bw.write("\n");
            }
            bw.close();
        }    
    }
    

    public void Partition2GiraphOblivious(String filename, String extension) throws IOException
    {
        String newfilename;
        String path = new java.io.File(".").getCanonicalPath();
        int totalVPrinted = 0, lastVstart = 0;
        
        // Dump the graph in the original order
        for (int c=0; c < clusters; c++)
        {
            newfilename = filename+"_Giraph_c"+clusters+"_"+c+ "." + extension;
            System.out.println("Dumping: " + newfilename);
            File f = new File(path, newfilename);
            f.createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));

            int length = (int) (G.V()/clusters);
            if (c == clusters-1)
            {
                length = G.V()-totalVPrinted;
            }

            lastVstart = totalVPrinted;
            while(totalVPrinted < lastVstart + length)
            {

                int key = order[totalVPrinted+1];
                int value = (Integer) partitionmap.get(key);
                bw.write(key+","+value);
                for(int i: G.adjacentTo(key))
                {
                    value = (Integer) partitionmap.get(i);
                    bw.write(","+i+","+value);
                }
                bw.write("\n");
                totalVPrinted++;
            }
            bw.close();
        }
    }

    
    
    /** This function prints the graph in a metis style format but with the following
     * difference. Each line contains the vertex id (first number in each line), its
     * assigned cluster (second number in each line, ranging from 0 to k-1) and then
     * the neighbourhood of that specific vertex.
     *
     */
    public void Partition2MetisFile(String filename) throws IOException
    {
        String newfilename = "MPartition"+sorder+"_Clusters"+clusters+"_"+filename+"."+Math.random();
        String path = new java.io.File(".").getCanonicalPath();
        File f = new File(path, newfilename);
        f.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        Iterator it = partitionmap.keySet().iterator();
        while( it.hasNext() )
        {
            int key = (Integer)it.next();
            int value = (Integer) partitionmap.get(key);
            bw.write(key+" "+value);
            for(int i: G.adjacentTo(key))
            {
                bw.write(" "+i);
            }
            bw.write("\n");
        }
        bw.close();
    }
    public StreamingPartitioner(Graph G, int k)
    {
        super();
        this.G=G;
        clusters = k;
        order = G.defaultOrder();
        n = G.V();
        m = G.E();
        partitionmap = new HashMap<Integer,Integer>();
        vsets = new ArrayList<HashSet<Integer>>();
        HashSet hs;
        for(int i=0; i<k;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
    }
    
    public int numClusters()
    {
        return vsets.size();
    }
    
    /** This function sets the stream order to BFS, DFS or random according to
     * the string passed as argument
     */
    public void setStreamOrder(String streamorder)
    {
        if( streamorder.equalsIgnoreCase("BFS"))
        {
            BreadthFirstSearch BFS = new BreadthFirstSearch(G);
            BFS.getBFSorder(G);
            order = BFS.bfsorder;
            sorder = "BFS";
            return;
        }
        if( streamorder.equalsIgnoreCase("DFS"))
        {
            DepthFirstSearch DFS = new DepthFirstSearch(G);
            DFS.getDFSorder(G);
            order = DFS.dfsorder;
            sorder = "DFS";
            return;
        }
        if( streamorder.equalsIgnoreCase("Random"))
        {
            RandomOrder RO = new RandomOrder(G);
            RO.getRandomOrder();
            order = RO.randomorder;
            sorder = "Random";
            return;
        }
    }
    
    public void DegreePartitioning()
    {
        //HashSet S = new HashSet();
        int v;
        loadN = new int[clusters];
        Arrays.fill(loadN, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double max = -Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                //  System.out.print("Neighbors of v ");
                //  for( int w: G.adjacentTo(v))
                //      System.out.print(w+" ");
                //  System.out.append("");
                //  System.out.println(" Degree to cluster "+counter+" is "+degS[counter]);
                double score = degS[counter];
                if(  score >= max ){
                    if(Math.abs(score-max)<=ERR){
                        al.add(counter);
                        max = score;
                    }
                    else
                    {
                        al.clear();
                        al.add(counter);
                        max = score;
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
                winner = (Integer) al.remove(0);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadN[counter] +=1;
                }
                else
                {
                    fcutN += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeN = (endTime - startTime);
        fcutN = fcutN/(double)m;
        maxloadN = (double)findMax(loadN)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Degree Partitioning ");
        System.out.println(" Graph " + G.GetFilename());
        System.out.println(" NoClusters = " + clusters);
        printLoads(loadN);
        System.out.println("Timing nonneighbors: "+timeN+" milliseconds");
        System.out.println("Fraction of edges cut "+fcutN);
        System.out.println("Max load (normalized) "+ maxloadN);
        
    }
    
    
    public void LinearWeightedDegreePartitioning()
    {
        int v;
        int vertexStep;
        int vertexCnt = 0;
        loadLN = new int[clusters];
        Arrays.fill(loadLN, 0);
        double weight;
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        
        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }

        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("LinearWeightedDegreePartitioning(" + G.GetFilename() + ", " + clusters + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }

            int degS[] =  new int[clusters];
            int counter = 0;
            double max = -Double.MAX_VALUE;
            double maxWeight = -Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            ArrayList almw = new ArrayList();

            boolean useStantonCode = true;
            
            
            if (!useStantonCode)
            {
                for( HashSet S:  vsets)
                {
                    degS[counter]=G.InducedDegree_FAST(S, v);
                    weight = (1- loadLN[counter]*(double)clusters/(double)G.V());
                    double score = degS[counter]* weight;
                    if(  score >= max ){
                        if(Math.abs(score-max)<=ERR){
                            al.add(counter);
                            max = score;
                        }
                        else
                        {
                            al.clear();
                            al.add(counter);
                            max = score;
                        }
                    }
                    counter++;
                }
            }
            else
            {
                for( HashSet S:  vsets)
                {
                    degS[counter]=G.InducedDegree_FAST(S, v);
                    weight = (1- loadLN[counter]*(double)clusters/(double)G.V());
                    double score = degS[counter]* weight;
                    if(  score > max && weight > 0){
                        if(Math.abs(score-max)<=ERR){
                            al.add(counter);
                            max = score;
                        }
                        else
                        {
                            al.clear();
                            al.add(counter);
                            max = score;
                        }
                    }

                    // Find maxWeight to be used if no partition has any cross edges
                    if (weight >= maxWeight)
                    {
                        if (weight > maxWeight) 
                        {
                            almw.clear();
                            maxWeight = weight;
                        }
                        almw.add(counter);
                    }

                    counter++;
                }

                if (max == 0)
                {
                    // there are no edges to any partition, so assign randomly to max penalty
                    al = almw;
                }
            }
            
            
            
            
            if( al.size() > 1)
            {
                
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                if(!useRandomness)
                {
                    w2 = 0;
                }
                
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
                winner = (Integer) al.remove(0);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadLN[counter] +=1;
                }
                else
                {
                    fcutLN += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeLN = (endTime - startTime);
        fcutLN = fcutLN/(double)m;
        maxloadLN = (double)findMax(loadLN)*(double)clusters/(double)n;
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println(" Linear Weighted Degree Partitioning ");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            printLoads(loadLN);
            System.out.println("Timing nonneighbors: "+timeLN+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutLN);
            System.out.println("Max load (normalized) "+ maxloadLN);
        }
    }
    
    
    /** Exponential weighted partitioning */
    public void ExponentialWeightedDegreePartitioning()
    {
        int v;
        loadEWN = new int[clusters];
        partitionmap.clear();
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        Arrays.fill(loadEWN, 0);
        double weight;
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];
            //System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double max = -Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                weight = (1- Math.pow(EXP, loadEWN[counter]-(double)G.V()/(double)clusters ) );
                double score = degS[counter]* weight;
                if(  score >= max ){
                    if(Math.abs(score-max)<=ERR){
                        al.add(counter);
                        max = score;
                    }
                    else
                    {
                        al.clear();
                        al.add(counter);
                        max = score;
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
                winner = (Integer) al.remove(0);
            //System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadEWN[counter] +=1;
                }
                else
                {
                    // System.out.println(" Vertex v "+v+" went to cluster "+winner+" so cost for cluster "+counter+" is "+ degS[counter]);
                    fcutEWN+= degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeEWN = (endTime - startTime);
        
        fcutEWN = fcutEWN/(double)m;
        maxloadEWN = (double)findMax(loadEWN)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Exponential Weighted Degree Partitioning ");
        printLoads(loadEWN);
        System.out.println("Timing nonneighbors: "+timeEWN+" milliseconds");
        System.out.println("Fraction of edges cut "+fcutEWN);
        System.out.println("Max load (normalized) "+ maxloadEWN);
    }
    
    
    /** Hashing partitioning */
    public void HashingPartitioning()
    {
        int v;
        int vertexStep;
        int vertexCnt = 0;
        loadH = new int[clusters];
        Arrays.fill(loadH, 0);
        eLoadH = new int[clusters];
        Arrays.fill(eLoadH, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }

        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        
        double weight;
        Random randomGenerator = new Random();
        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("HashingPartitioning: " + i + "-th vertex arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }

            int winner = randomGenerator.nextInt(clusters);
            int degS[] =  new int[clusters];
            int counter = 0;
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                counter++;
            }
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadH[counter] +=1;
                    
                    // As Stanton&Kliot, we define the edge load to be the total 
                    // number of edges processed by the cluster
                    eLoadH[counter] += G.degree(v);
                }
                else
                {
                    fcutH+= degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeH = (endTime - startTime);
        fcutH=fcutH/(double)m;
        maxloadH = (double)findMax(loadH)*(double)clusters/(double)n;
        
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println(" Hashing Partitioning ");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            System.out.print(" loadH =" );
            printLoads(loadH);
            System.out.print(" eloadH =" );
            printLoads(eLoadH);
            System.out.println("Timing nonneighbors: "+timeH+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutH);
            System.out.println("Max load (normalized) "+ maxloadH);
        }
        
        
        
        
    }
    
    
    /** Balanced partitioning */
    public void BalancedPartitioning()
    {
        int v;
        loadB = new int[clusters];
        Arrays.fill(loadB, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];
            int winner=0;
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                double score = loadB[counter];
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
                winner = (Integer) al.remove(0);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadB[counter] +=1;
                }
                else
                {
                    fcutB+= degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeB = (endTime - startTime);
        fcutB = fcutB/(double)m;
        maxloadB = (double)findMax(loadB)*(double)clusters/(double)n;
        System.out.println("Total number of edges cut "+fcutB+" max load "+ maxloadB);
        
        System.out.println("***********************************************");
        System.out.println(" Balanced Partitioning ");
        printLoads(loadB);
        System.out.println("Timing nonneighbors: "+timeB+" milliseconds");
        System.out.println("Fraction of edges cut "+fcutB);
        System.out.println("Max load (normalized) "+ maxloadB);
        
    }
    
    
    public void TrianglePartitioning()
    {
        int v;
        loadT = new int[clusters];
        Arrays.fill(loadT, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];
            int degS[] =  new int[clusters];
            int triangleS[] =  new int[clusters];
            int counter = 0;
            double max = -Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                triangleS[counter] = G.InducedTriangles_FAST(S, v);
                double score = triangleS[counter];
                if( score >= max ){
                    
                    if(Math.abs(score-max)<=ERR){
                        al.add(counter);
                        max = score;
                    }
                    else
                    {
                        al.clear();
                        al.add(counter);
                        max = score;
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
                winner = (Integer) al.remove(0);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadT[counter] +=1;
                }
                else
                {
                    fcutT += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeT = (endTime - startTime);
        printLoads(loadT);
        fcutT = fcutT/(double)m;
        maxloadT = (double)findMax(loadT)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Triangle Partitioning ");
        printLoads(loadT);
        System.out.println("Timing nonneighbors: "+timeT+" milliseconds");
        System.out.println("Fraction of edges cut "+fcutT);
        System.out.println("Max load (normalized) "+ maxloadT );
        
        
    }
    
    public void LinearWeightedTrianglePartitioning()
    {
        int v;
        loadLT = new int[clusters];
        Arrays.fill(loadLT, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            double weight;
            v = order[i];
            int degS[] =  new int[clusters];
            int triangleS[] =  new int[clusters];
            int counter = 0;
            double max = -Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                triangleS[counter] = G.InducedTriangles_FAST(S, v);
                weight = (1- loadLT[counter]*(double)clusters/(double)n);
                double score = triangleS[counter]*weight ;
                if( score >= max ){
                    
                    if(Math.abs(score-max)<=ERR){
                        al.add(counter);
                        max = score;
                    }
                    else
                    {
                        al.clear();
                        al.add(counter);
                        max = score;
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
                winner = (Integer) al.remove(0);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadLT[counter] +=1;
                }
                else
                {
                    fcutLT += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeLT = (endTime - startTime);
        
        fcutLT = fcutLT/(double)m;
        maxloadLT = (double)findMax(loadLT)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Linearly Weighted Triangle Partitioning ");
        printLoads(loadLT);
        System.out.println("Timing nonneighbors: "+timeLT+" milliseconds");
        System.out.println("Fraction of edges cut "+fcutLT);
        System.out.println("Max load (normalized) "+ maxloadLT );
    }
    
    
    public void ExponentialWeightedTrianglePartitioning()
    {
        int v;
        loadEWT = new int[clusters];
        Arrays.fill(loadEWT, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            double weight;
            v = order[i];
            int degS[] =  new int[clusters];
            int triangleS[] =  new int[clusters];
            int counter = 0;
            double max = -Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                triangleS[counter] = G.InducedTriangles_FAST(S, v);
                weight = (1- Math.pow(EXP, loadEWT[counter]-(double)n/(double)clusters ) );
                double score = triangleS[counter]*weight;
                if(  score >= max ){
                    if(Math.abs(score-max)<=ERR){
                        al.add(counter);
                        max = score;
                    }
                    else
                    {
                        al.clear();
                        al.add(counter);
                        max = score;
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
                winner = (Integer) al.remove(0);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadEWT[counter] +=1;
                }
                else
                {
                    fcutEWT += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeEWT = (endTime - startTime);
        System.out.println(" Elapsed time "+timeEWT+" milliseconds");
        
        fcutEWT = fcutEWT/(double)m;
        maxloadEWT = (double)findMax(loadEWT)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Exponential Weighted Triangle Partitioning ");
        printLoads(loadEWT);
        System.out.println("Timing nonneighbors: "+timeEWT+" milliseconds");
        System.out.println("Fraction of edges cut "+fcutEWT);
        System.out.println("Max load (normalized) "+ maxloadEWT );
        
        
    }
    
    
    public void NonNeighborsPartitioning()
    {
        int v;
        loadNN = new int[clusters];
        Arrays.fill(loadNN, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                double score = loadNN[counter] - degS[counter];
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
                winner = (Integer) al.remove(0);
            // System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadNN[counter] +=1;
                }
                else
                {
                    fcutNN += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeNN = (endTime - startTime);
        fcutNN = fcutNN/(double)m;
        maxloadNN = (double)findMax(loadNN)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Nonneighbors partitioning");
        printLoads(loadNN);
        System.out.println("Timing nonneighbors: "+timeNN+" milliseconds");
        System.out.println("Fraction of edges cut "+fcutNN);
        System.out.println("Max load (normalized) "+ maxloadNN );
    }





  public void NonNeighborsConvexfNormalized(double gamma)
    {
        int v;
        int vertexStep;
        int vertexCnt = 0;
        loadNfN = new int[clusters];
        Arrays.fill(loadNfN, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        
        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        
        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("NonNeighborsConvexfNormalized(" + G.GetFilename() + ", " + clusters + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }
            
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                double score = G.E()/Math.pow(G.V(),gamma)* Math.pow(clusters,gamma-1) * ( Math.pow(loadNfN[counter]+1,gamma)-Math.pow(loadNfN[counter],gamma) ) -degS[counter] ;
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
                    //System.out.println("Cluster "+counter+" currently winner, min now is "+min);
                }
                counter++;
            }
            if( al.size() > 1)
            {
                //System.out.println("Resolving ties");
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                if(!useRandomness)
                {
                    w2 = 0;
                }
                
                counter = 0;
                int counter2=0;
                for( HashSet S:  vsets)
                {
                    if( al.contains((Integer)counter))
                    {
                        if( w2 == counter2 )
                            winner = counter;
                        counter2++;
                    }
                    counter++;
                }
            }
            else
                winner = (Integer) al.remove(0);
            //System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadNfN[counter] +=1;
                }
                else
                {
                    fcutNfN += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeNfN = (endTime - startTime);        
        fcutNfN = fcutNfN/m;
        maxloadNfN = (double)findMax(loadNfN)*(double)clusters/(double)n;
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println(" Convex  partitioning (gamma= "+gamma+"), NORMALIZED");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            printLoads(loadNfN);
            System.out.println("Timing nonneighbors: "+timeNfN+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutNfN);
            System.out.println("Max load (normalized) "+ maxloadNfN );
        }
    }
        
        


    
    public void NonNeighborsAlphaPartitioning(double alpha)
    {
        int v;
        loadNalpha = new int[clusters];
        Arrays.fill(loadNalpha, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }
        long startTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                double score = alpha*loadNalpha[counter] - degS[counter];
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
                winner = (Integer) al.remove(0);
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadNalpha[counter] +=1;
                }
                else
                {
                    fcutNalpha += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeNalpha = (endTime - startTime);
        fcutNalpha = fcutNalpha/(double)m;
        maxloadNalpha = (double)findMax(loadNalpha)*(double)clusters/(double)n;
        
        System.out.println("***********************************************");
        System.out.println(" Quadratic  partitioning (alpha= "+alpha+")");
        printLoads(loadNalpha);
        System.out.println("Timing nonneighbors: "+timeNalpha+" milliseconds");
        System.out.println("Fraction of edges cut "+fcutNalpha);
        System.out.println("Max load (normalized) "+ maxloadNalpha );
    }
    
    public void NonNeighborsConvexf(double gamma)
    {
        int v;
        int vertexStep;
        int vertexCnt = 0;
        loadNf = new int[clusters];
        Arrays.fill(loadNf, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }

        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        
        long startTime = System.currentTimeMillis();
        long lastTime = startTime;
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];
            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("NonNeighborsConvexf(" + G.GetFilename() + ", " + clusters + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                double score = 1/gamma* ( Math.pow(loadNf[counter]+1,gamma)-Math.pow(loadNf[counter],gamma) ) -degS[counter] ;
                //System.out.println("(s_"+counter+"+1)^gamma="+Math.pow(loadNf[counter]+1,gamma));
                //  System.out.println("s_"+counter+"^gamma="+Math.pow(loadNf[counter],gamma));
                //   System.out.println(" deg_"+counter+"("+v+")="+degS[counter]);
                //   System.out.println("Cluster "+counter+" has "+loadNf[counter]+" load and the score for vertex"+v+" to get there is "+score);
                if( score <= min ){
                    //System.out.println("Math.abs(min-score) "+Math.abs(min-score));
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
                    //System.out.println("Cluster "+counter+" currently winner, min now is "+min);
                }
                counter++;
            }
            if( al.size() > 1)
            {
                //System.out.println("Resolving ties");
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                if(!useRandomness)
                {
                    w2 = 0;
                }
                
                counter = 0;
                int counter2=0;
                for( HashSet S:  vsets)
                {
                    if( al.contains((Integer)counter))
                    {
                        if( w2 == counter2 )
                            winner = counter;
                        counter2++;
                    }
                    counter++;
                }
            }
            else
                winner = (Integer) al.remove(0);

            
            partitionmap.put(v, winner);
            counter = 0;
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadNf[counter] +=1;
                }
                else
                {
                    fcutNf += degS[counter];
                }
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeNf = (endTime - startTime);
        
        fcutNf = fcutNf/(double)m;
        maxloadNf = (double)findMax(loadNf)*(double)clusters/(double)n;
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println(" Convex  partitioning (gamma= "+gamma+")");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            printLoads(loadNf);
            System.out.println("Timing nonneighbors: "+timeNf+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutNf);
            System.out.println("Max load (normalized) "+ maxloadNf );
        }
    }

    

    public void NonNeighborsConvexfKnee(double gamma, double threshold)
    {
        int v;
        int vertexCnt = 0, vertexStep;
        loadNfKnee = new int[clusters];
        Arrays.fill(loadNfKnee, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }

        
        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        
        
        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("NonNeighborsConvexfKnee(" + G.GetFilename() + ", " + clusters + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }

            
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                if(loadNfKnee[counter]<=threshold*n/clusters)
                {
                    double score = 1/gamma* ( Math.pow(loadNfKnee[counter]+1,gamma)-Math.pow(loadNfKnee[counter],gamma) ) -degS[counter] ;
                    if( score <= min ){
                        //System.out.println("Math.abs(min-score) "+Math.abs(min-score));
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
                    
                }
                counter++;
            }
            if( al.size() > 1)
            {
                //System.out.println("Resolving ties");
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                counter = 0;
                int counter2=0;
                for( HashSet S:  vsets)
                {
                    if( al.contains((Integer)counter))
                    {
                        if( w2 == counter2 )
                            winner = counter;
                        counter2++;
                    }
                    counter++;
                }
            }
            else
                winner = (Integer) al.remove(0);
            //System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;
            
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadNfKnee[counter] +=1;
                }
                else
                {
                    fcutNfKnee += degS[counter];
                }
                
                
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeNfKnee = (endTime - startTime);
        
        fcutNfKnee = fcutNfKnee/m;
        maxloadNfKnee = (double)findMax(loadNfKnee)*(double)clusters/(double)n;
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println("Knee Convex  partitioning (gamma= "+gamma+",thr=" + threshold + ")");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            printLoads(loadNfKnee);
            System.out.println("Timing nonneighbors: "+timeNfKnee+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutNfKnee);
            System.out.println("Max load (normalized) "+ maxloadNfKnee );
        }        
    }
        


    
    
    public void NonNeighborsConvexfNormalizedKnee(double gamma, double threshold)
    {
        int v;
        int vertexCnt = 0, vertexStep;
        loadNfNKnee = new int[clusters];
        Arrays.fill(loadNfNKnee, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }

        
        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        
        
        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("NonNeighborsConvexfNormalizedKnee(" + G.GetFilename() + ", " + clusters + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }

            
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                if(loadNfNKnee[counter]<=threshold*n/clusters)
                {
                    //double score = 1/gamma* ( Math.pow(loadNfNKnee[counter]+1,gamma)-Math.pow(loadNfNKnee[counter],gamma) ) -degS[counter] ;
                    double score = G.E()/Math.pow(G.V(),gamma)* Math.pow(clusters,gamma-1) * ( Math.pow(loadNfNKnee[counter]+1,gamma)-Math.pow(loadNfNKnee[counter],gamma) ) -degS[counter] ;
                    if( score <= min ){
                        //System.out.println("Math.abs(min-score) "+Math.abs(min-score));
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
                    
                }
                counter++;
            }
            if( al.size() > 1)
            {
                //System.out.println("Resolving ties");
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                counter = 0;
                int counter2=0;
                for( HashSet S:  vsets)
                {
                    if( al.contains((Integer)counter))
                    {
                        if( w2 == counter2 )
                            winner = counter;
                        counter2++;
                    }
                    counter++;
                }
            }
            else
                winner = (Integer) al.remove(0);
            //System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;
            
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadNfNKnee[counter] +=1;
                }
                else
                {
                    fcutNfNKnee += degS[counter];
                }
                
                
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeNfNKnee = (endTime - startTime);
        
        fcutNfNKnee = fcutNfNKnee/m;
        maxloadNfNKnee = (double)findMax(loadNfNKnee)*(double)clusters/(double)n;
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println("Normalized Knee Convex partitioning (gamma= "+gamma+",thr=" + threshold + ")");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            printLoads(loadNfNKnee);
            System.out.println("Timing nonneighbors: "+timeNfNKnee+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutNfNKnee);
            System.out.println("Max load (normalized) "+ maxloadNfNKnee );
        }        
    }
        

    
    
    
    public void printPartition()
    {
        Iterator it = partitionmap.keySet().iterator();
        while( it.hasNext() )
        {
            int key = (Integer)it.next();
            int value = (Integer) partitionmap.get(key);
            System.out.println(key+" "+value);
        }
    }
    
    
    /** Returns the maximum value from an array v */
    private int findMax(int[] v)
    {
        //   System.out.println("length of v "+v.length);
        int max = v[0];
        //int ind = 0;
        for(int i =1;i< v.length;i++)
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
    private int findMin(int[] v)
    {
        int min = v[0];
        //int ind = 0;
        for(int i =1;i< v.length;i++)
        {
            if( min>v[i])
            {
                min = v[i];
                // ind = i;
            }
        }
        return min;
    }
    
    private void printLoads(int[] v)
    {
        
        // System.out.println("**LOADS**");
        System.out.print("  |");
        for(int i=0; i<v.length; i++)
        {
            System.out.print(v[i]+"|");
        }
        System.out.println("");
        // System.out.println("*********");
    }
    
    
    
    
 
    
    
    
    
    public void NonNeighborsConvexfNormalizedKneeEdges(double gamma, double threshold)
    {
        int v;
        int vertexCnt = 0, vertexStep;
        loadNfNKneeE = new int[clusters];
        Arrays.fill(loadNfNKneeE, 0);
        eLoadNfNKneeE = new int[clusters];
        Arrays.fill(eLoadNfNKneeE, 0);

        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }

        
        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        
        
        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("NonNeighborsConvexfNormalizedKneeEdges(" + G.GetFilename() + ", " + clusters + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }
           
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                //if(loadNfNKneeE[counter]<=threshold*n/clusters)
                if(eLoadNfNKneeE[counter]<=threshold*2*m/clusters)
                {
                    //double score = 1/gamma* ( Math.pow(loadNfNKneeE[counter]+1,gamma)-Math.pow(loadNfNKneeE[counter],gamma) ) -degS[counter] ;
                    //double score = G.E()/Math.pow(G.V(),gamma)* Math.pow(clusters,gamma-1) * ( Math.pow(loadNfNKneeE[counter]+1,gamma)-Math.pow(loadNfNKneeE[counter],gamma) ) -degS[counter] ;
                    
                    //double score = G.E()/Math.pow(2*G.E(),gamma)* Math.pow(clusters,gamma-1) * ( Math.pow(eLoadNfNKneeE[counter]+1,gamma)-Math.pow(eLoadNfNKneeE[counter],gamma) ) - degS[counter] ;
                    double score = gamma * G.E()/Math.pow(2*G.E(),gamma)* Math.pow(clusters,gamma) * ( Math.pow(eLoadNfNKneeE[counter]+1,gamma)-Math.pow(eLoadNfNKneeE[counter],gamma) ) - degS[counter] ;
                    if( score <= min ){
                        //System.out.println("Math.abs(min-score) "+Math.abs(min-score));
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
                    
                }
                counter++;
            }
            if( al.size() > 1)
            {
                //System.out.println("Resolving ties");
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                counter = 0;
                int counter2=0;
                for( HashSet S:  vsets)
                {
                    if( al.contains((Integer)counter))
                    {
                        if( w2 == counter2 )
                            winner = counter;
                        counter2++;
                    }
                    counter++;
                }
            }
            else
                winner = (Integer) al.remove(0);
            //System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;
            
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadNfNKneeE[counter] +=1;
                    //eLoadNfNKneeE[counter] += degS[counter];
                    
                    // As Stanton&Kliot, we define the edge load to be the total 
                    // number of edges processed by the cluster
                    eLoadNfNKneeE[counter] += G.degree(v);
                }
                else
                {
                    fcutNfNKneeE += degS[counter];
                }
                
                
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeNfNKneeE = (endTime - startTime);
        
        fcutNfNKneeE = fcutNfNKneeE/m;
        //maxloadNfNKneeE = (double)findMax(loadNfNKneeE)*(double)clusters/(double)n;
        maxloadNfNKneeE = (double)findMax(eLoadNfNKneeE)*(double)clusters/(double)m/2.0;
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println("Normalized Knee Edges Convex partitioning (gamma= "+gamma+",thr=" + threshold + ")");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            System.out.print(" loadNfNKneeE =" );
            printLoads(loadNfNKneeE);
            System.out.print(" eLoadNfNKneeE =" );
            printLoads(eLoadNfNKneeE);
            System.out.println("Timing nonneighbors: "+timeNfNKneeE+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutNfNKneeE);
            System.out.println("Max load (normalized) "+ maxloadNfNKneeE );
        }        
    }
        
 
    
    public void NonNeighborsConvexfKneeEdges(double gamma, double threshold)
    {
        int v;
        int vertexCnt = 0, vertexStep;
        loadNfKneeE = new int[clusters];
        Arrays.fill(loadNfKneeE, 0);
        eLoadNfKneeE = new int[clusters];
        Arrays.fill(eLoadNfKneeE, 0);
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }

        
        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        
        
        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("NonNeighborsConvexfKnee_Edge(" + G.GetFilename() + ", " + clusters + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }

            
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double min = Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                //if(loadNfKneeE[counter]<=threshold*n/clusters)
                if(eLoadNfKneeE[counter]<=threshold*2*m/clusters)
                {
                    double score = 1/gamma* ( Math.pow(eLoadNfKneeE[counter]+1,gamma)-Math.pow(eLoadNfKneeE[counter],gamma) ) - degS[counter] ;
                    if( score <= min ){
                        //System.out.println("Math.abs(min-score) "+Math.abs(min-score));
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
                    
                }
                counter++;
            }
            
            
            
            if( al.size() > 1)
            {
                //System.out.println("Resolving ties");
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                counter = 0;
                int counter2=0;
                for( HashSet S:  vsets)
                {
                    if( al.contains((Integer)counter))
                    {
                        if( w2 == counter2 )
                            winner = counter;
                        counter2++;
                    }
                    counter++;
                }
            }
            else
                winner = (Integer) al.remove(0);
            //System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;
            
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadNfKneeE[counter] +=1;
                    //eLoadNfKneeE[counter] += degS[counter];

                    // As Stanton&Kliot, we define the edge load to be the total 
                    // number of edges processed by the cluster
                    eLoadNfKneeE[counter] += G.degree(v);
               }
                else
                {
                    fcutNfKneeE += degS[counter];
                }
                
                
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeNfKneeE = (endTime - startTime);
        
        fcutNfKneeE = fcutNfKneeE/m;
        //maxloadNfKneeE = (double)findMax(loadNfKneeE)*(double)clusters/(double)n;
        maxloadNfKneeE = (double)findMax(eLoadNfKneeE)*(double)clusters/(double)m/2.0;
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println("Knee Convex Edges partitioning (gamma= "+gamma+",thr=" + threshold + ")");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            System.out.print(" loadNfKneeE =" );
            printLoads(loadNfKneeE);
            System.out.print(" eLoadNfKneeE =" );
            printLoads(eLoadNfKneeE);
            System.out.println("Timing nonneighbors: "+timeNfKneeE+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutNfKneeE);
            System.out.println("Max load (normalized) "+ maxloadNfKneeE );
        }        
    }
        
 
    
    
   public void LinearWeightedDegreePartitioningEdges()
    {
        int v;
        int vertexCnt = 0, vertexStep;
        loadLNE = new int[clusters];
        Arrays.fill(loadLNE, 0);
        eLoadLNE = new int[clusters];
        Arrays.fill(eLoadLNE, 0);
        double weight;
        vsets.clear();
        HashSet hs;
        for(int i=0; i<clusters;i++)
        {
            hs = new HashSet<Integer>();
            vsets.add(hs);
        }

        
        if (G.V() > 100000)
        {
            vertexStep = G.V()/100;
        }
        else
        {
            vertexStep = G.V()/10;            
        }
        
        
        long startTime = System.currentTimeMillis();
        long lastTime = System.currentTimeMillis();
        for(int i = 1; i<= G.V();i++)
        {
            v = order[i];

            if (vertexCnt < i)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("LinearWeightedDegreePartitioning_Edges(" + G.GetFilename() + ", " + clusters + "): " + i + "-th vertex (" + (double)(vertexCnt) / G.V() + "%) arrived after " + delta);
                lastTime = System.currentTimeMillis();
                vertexCnt += vertexStep;
            }

/* Our initial interpretation of Stanton-Kliot code            
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double max = -Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            for( HashSet S:  vsets)
            {
                degS[counter]=G.InducedDegree_FAST(S, v);
                weight = (1- eLoadLNE[counter]*(double)clusters/(double)G.E());
                double score = degS[counter]* weight;
                if(  score >= max ){
                    if(Math.abs(score-max)<=ERR){
                        al.add(counter);
                        max = score;
                    }
                    else
                    {
                        al.clear();
                        al.add(counter);
                        max = score;
                    }
                }
                    
                counter++;
            }
            if( al.size() > 1)
            {
                //System.out.println("Resolving ties");
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                counter = 0;
                int counter2=0;
                for( HashSet S:  vsets)
                {
                    if( al.contains((Integer)counter))
                    {
                        if( w2 == counter2 )
                            winner = counter;
                        counter2++;
                    }
                    counter++;
                }
            }
            else
                winner = (Integer) al.remove(0);
            //System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;
*/
            
            
            

            
/* Stanton-Kliot code as described by Isabelle in an email */
            
            //   System.out.println("Vertex "+ v+" arrived");
            int degS[] =  new int[clusters];
            int counter = 0;
            double max = -Double.MAX_VALUE;
            double maxWeight = -Double.MAX_VALUE;
            int winner = 0;
            ArrayList al = new ArrayList();
            ArrayList almw = new ArrayList();
 
            
            
            boolean useStantonCode = true;
            
            
            if (!useStantonCode)
            {
                for( HashSet S:  vsets)
                {
                    degS[counter]=G.InducedDegree_FAST(S, v);
                    weight = (1- eLoadLNE[counter]*(double)clusters/(double)G.E()/2.0);
                    double score = degS[counter]* weight;
                    if(  score >= max ){
                        if(Math.abs(score-max)<=ERR){
                            al.add(counter);
                            max = score;
                        }
                        else
                        {
                            al.clear();
                            al.add(counter);
                            max = score;
                        }
                    }
                    counter++;
                }
            }
            else
            {
                for( HashSet S:  vsets)
                {
                    degS[counter]=G.InducedDegree_FAST(S, v);
                    weight = (1- eLoadLNE[counter]*(double)clusters/(double)G.E()/2.0);
                    double score = degS[counter]* weight;
                    if(  score > max && weight > 0){
                        if(Math.abs(score-max)<=ERR){
                            al.add(counter);
                            max = score;
                        }
                        else
                        {
                            al.clear();
                            al.add(counter);
                            max = score;
                        }
                    }

                    // Find maxWeight to be used if no partition has any cross edges
                    if (weight >= maxWeight)
                    {
                        if (weight > maxWeight) 
                        {
                            almw.clear();
                            maxWeight = weight;
                        }
                        almw.add(counter);
                    }

                    counter++;
                }

                if (max == 0)
                {
                    // there are no edges to any partition, so assign randomly to max penalty
                    al = almw;
                }
            }
            
            if( al.size() > 1)
            {
                //System.out.println("Resolving ties");
                Random rr = new Random();
                int w2 = rr.nextInt(al.size());
                
                counter = 0;
                int counter2=0;
                for( HashSet S:  vsets)
                {
                    if( al.contains((Integer)counter))
                    {
                        if( w2 == counter2 )
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
            //System.out.println(" Vertex "+ v+" goes to cluster "+ winner);
            partitionmap.put(v, winner);
            counter = 0;

            
            
            
            
            
            
            for( HashSet S:  vsets)
            {
                
                if( counter == winner )
                {
                    S.add(v);
                    loadLNE[counter] +=1;
                    //eLoadLNE[counter] += degS[counter];

                    // As Stanton&Kliot, we define the edge load to be the total 
                    // number of edges processed by the cluster
                    eLoadLNE[counter] += G.degree(v);
               }
                else
                {
                    fcutLNE += degS[counter];
                }
                
                
                counter++;
            }
            
        }
        long endTime = System.currentTimeMillis();
        timeLNE = (endTime - startTime);
        
        fcutLNE = fcutLNE/m;
        //maxloadLNE = (double)findMax(loadLNE)*(double)clusters/(double)n;
        maxloadLNE = (double)findMax(eLoadLNE)*(double)clusters/(double)m/2.0;
        synchronized(System.out)
        {
            System.out.println("***********************************************");
            System.out.println("Linear Weighted Degree Edges partitioning");
            System.out.println(" Graph " + G.GetFilename());
            System.out.println(" NoClusters = " + clusters);
            System.out.print(" loadLNE =" );
            printLoads(loadLNE);
            System.out.print(" eLoadLNE =" );
            printLoads(eLoadLNE);
            System.out.println("Timing nonneighbors: "+timeLNE+" milliseconds");
            System.out.println("Fraction of edges cut "+fcutLNE);
            System.out.println("Max load (normalized) "+ maxloadLNE );
        }        
    }
     

    
}