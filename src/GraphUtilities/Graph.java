package GraphUtilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class Graph {
    protected String filename;
    
    // number of edges
    protected long E;
    
    // number of vertices
    protected int V;
    
    protected int[] BFSORDER; // BFS ordering of vertices starting from a randomly chosen vertex
    protected int[] DFSORDER; // DFS ordering of vertices starting from a randomly chosen vertex
    protected int[] RANDOMORDER; // random ordering of vertices starting from a randomly chosen vertex
    
    protected double alpha;   
    protected double gamma; 
    
    protected String delimiter;
    
    public void setAlpha(double a){alpha = a;}
    public void setGamma(double g){gamma =g;}
 
    /* Used for displaying diagnostic problems */
    protected static void DEBUGMSG(String s)
    {
        System.out.println(s);
    }
    
    /**
     * Create an empty graph with no vertices or edges.
     */
    public Graph()
    {
    }

    
    protected void ImportGraph(String edgefile, String delimiter) throws IOException
    {
        ImportGraph(edgefile, delimiter, true);
    }

    protected void ImportGraph(String edgefile, String delimiter, boolean useHeader) throws IOException
    {
        BufferedReader br;
        String line;
        String[] result;
        // the first line of the file contains the number of vertices and then
        // the number of edges
        //V = Integer.parseInt(result[0]);
        //int ignore = Integer.parseInt(result[1]);

        filename = edgefile;
        br = new BufferedReader(new FileReader(edgefile));

        if (useHeader)
        {
            line = br.readLine();
            result = line.split(delimiter);
            System.out.println("Importign graph with headers");
        }
        else
        {
            System.out.println("Importign graph without headers");
        }

        
        
        line = br.readLine();
        int counter=0;
        long lastTime = System.currentTimeMillis();
        while( line!= null )
        {
            counter++;
            
            
            // BOZIDAR-DEBUG
            // Subsample 50% of edges
            //Random rr = new Random();
            //if (rr.nextInt(2) == 1)
            {
                result = line.split(delimiter);
                int u = Integer.parseInt(result[0]);
                int v = Integer.parseInt(result[1]);
                addEdge(u,v);
            }
            
            line = br.readLine();
            
            if (counter % 10000000 == 0)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("Reading and storing line " + counter + " after " + delta);
                lastTime = System.currentTimeMillis();
            }
        }
        /*
        if( ignore != counter) {
            DEBUGMSG("Check, #edges not equal to number of lines read");
        }
        */
        
        /*
        if( V!= st.size()){
            //System.out.println( " V "+V+" st.size "+st.size());
            System.out.println("G is not connected ");
        }
        */
        br.close();        
    }


    protected void MTParseLine(String line)
    {
        String[] result;
        result = line.split(delimiter);
        int u = Integer.parseInt(result[0]);
        int v = Integer.parseInt(result[1]);
        MTaddEdge(u,v);
    }
    
    protected void MTImportGraph(String edgefile, String del, int numberOfThreads) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(edgefile));
        String line = br.readLine();
        ArrayList<MTGraphParser> Sites;
        //int lineStep = 100000;
        //int timeDelay = 1000;
        int lineStep = 10000000;
        int timeDelay = 60000;
        delimiter = del;
        
        // Line queue for multi-threaded processing
        BlockingQueue<ArrayList<String>> publicLinesQueue;    
        publicLinesQueue = new LinkedBlockingQueue();                
        Sites = new ArrayList();
        for(int i=0; i<numberOfThreads; i++)
        {
            MTGraphParser worker;
            worker = new MTGraphParser(i, this, publicLinesQueue); 

            Sites.add(worker);
        }

        
        String[] result = line.split(delimiter);
        // the first line of the file contains the number of vertices and then
        // the number of edges
        V = Integer.parseInt(result[0]);
        int ignore = Integer.parseInt(result[1]);
      
        System.out.format("Total lines expected %d%n", ignore);
        
        double starttime = System.currentTimeMillis();
        double lasttime = starttime;

        line = br.readLine();
        int counter=0;
        int counter1=0;
        while( line!= null )
        {
            int localcnt = 0;
            ArrayList<String> lines = new ArrayList();
            while (localcnt < 100 && line != null)
            {
                counter++;
                lines.add(line);
                line = br.readLine();
                localcnt++;
            }
            publicLinesQueue.add(lines);
            counter1++;
            
            // bozidar DEBUG
            if (counter % lineStep == 0)
            {
                Double delta = System.currentTimeMillis() - lasttime;
                System.out.println("Reading line " + counter + " after " + delta.toString());
                lasttime = System.currentTimeMillis();
            }
            
        }
        
        if( ignore != counter) {
            DEBUGMSG("Check, #edges not equal to number of lines read");
        }

        System.out.format("Total lines %d, batches %d read in %f ms%n", 
                counter, counter1, System.currentTimeMillis() - starttime);

        

        lasttime = System.currentTimeMillis();
        
        // Wait until the queue is empty
        int counter2 = publicLinesQueue.size();
        try
        {
            while (!publicLinesQueue.isEmpty())
            {
                Thread.currentThread().sleep(timeDelay);
                int cntSize = publicLinesQueue.size();
                if (cntSize < counter2)
                {
                    Double delta = System.currentTimeMillis() - lasttime;
                    System.out.println("Remaining " + cntSize + " vertices, after " + delta.toString());
                    counter2 = cntSize - lineStep;
                    lasttime = System.currentTimeMillis();
                }
            }
            Thread.currentThread().sleep(1000);

            System.out.format("Graph threads %d stats: ", numberOfThreads);
            for(MTGraphParser t : Sites)
            {
                t.finish();
            }
            System.out.println("");
        
        }
        catch (InterruptedException e)
        {
        }

        System.out.format("Processed in %f ms%n", 
                System.currentTimeMillis() - starttime);
        
        br.close();        
    }
    
    
    /**
     * Create an graph from given input stream using given delimiter.
     */
    public Graph(String edgefile, String delimiter) throws IOException {
            ImportGraph(edgefile, delimiter);
    }
    
    public Graph(String edgefile, String delimiter, int noThreads) throws IOException {
        // To use multi-threaded parsing, use MTImportGraph
        if (noThreads == 0)
        {
            ImportGraph(edgefile, delimiter);
        }
        else
        {
            MTImportGraph(edgefile, delimiter, noThreads);
        }

    }
    
    /**
     * Return the set of neighbours of vertex v as in Iterable.
     */
    public abstract Iterable<Integer> adjacentTo(int v);
    
    /**
     * Add edge v-w to this graph (if it is not already an edge)
     */
    public abstract void addEdge(int v, int w);
    
    /**
     * Add vertex v to this graph (if it is not already a vertex)
     */
    public abstract void addVertex(int v);

    
    
    
    /**
     * Number of vertices.
     */
    public int V() {
        return V;
    }
    

    
    /**
     * Number of edges.
     */
    public long E() {
        return E;
    }
    
    /**
     * Degree of this vertex.
     * 
     * public int degree(int v) {
     * if (!st.contains(v)) throw new RuntimeException(v + " is not a vertex");
     * else return st.get(v).size();
     * } */
    
    
    /**
     * Degree of vertex v.
     */
    public abstract int degree(int v);
    
    
    
    public void Graph2EdgeFile( String filename) throws IOException
    {
        File f = new File(filename);
        f.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        for(int u = 1; u< V;u++ )
            for(int v=u+1; v<=V; v++)
                if( hasEdge(u,v))
                    bw.write(u+"\t"+v+"\n");
        bw.close();
    }
    
    
    
    public void Graph2MetisFile(String filename) throws IOException
    {
        File f = new File(filename);
        f.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        for(int u = 1; u<= V;u++ )
        {
            Iterable<Integer> myit = adjacentTo(u);
            Iterator it = myit.iterator();
            while(it.hasNext())
            {
                bw.write(it.next()+" ");             
            }
            bw.write("\n");
        }
        bw.close();
    }
    
    
    /**
     * This function returns the degree of vertex u in set S, which is indicated
     * by S_Hashset
     */   
    public abstract int InducedDegree_FAST(Collection<Integer> S_HashSet,int u);
    

    
    
    
    
    
    
    /**
     * Returns the number of edges in the graph induced by vertex set S
     * Vector v is an indicator vector
     * @param v
     * @return
     */
    public int InducedEdges(int v[])
    {
        int edges = 0;
        //for(int i = 0; i < v.length; i++)
        //  System.out.print(" "+v[i]);
        // System.out.println("");
        for(int i = 1;i<v.length-1;i++)
        {
            for(int j = i+1; j<= v.length-1;j++)
            {
                
                if(v[i]==1 && v[j]==1)
                {
                    //  System.out.println("Both "+i+" "+j+" are active");
                    if(hasEdge(i,j))
                    {
                        //  System.out.println("edge between "+i+" "+j);
                        edges+=1;
                    }
                }
            }
        }
        
        return edges;
    }
    
    

    
    
    
    public int InducedTriangles(int v[])
    {
        int triangles = 0;
        for(int i = 1; i<v.length-2;i++)
        {
            for(int j = i+1; j< v.length-1;j++)
            {
                for(int k=j+1;k<v.length;k++)
                {
                    if(v[i]==1 && v[j]==1 && v[k]==1)
                    {
                        if(hasEdge(i,j) && hasEdge(i,k) && hasEdge(j,k))
                        {
                            triangles+=1;
                        }
                    }
                }
            }
        }
        
        return triangles;
    }
    
    public abstract int InducedTriangles_FAST(Collection<Integer> S_HashSet);
    
    // to test
     public abstract int InducedTriangles_FAST(Collection<Integer> S_HashSet,int v);
     
    
   
    /**
     * Return the set of vertices as an Iterable.
     */
    // BOZIDAR-DEBUG
    /*
    public Iterable<Integer> vertices() {
        return st;
    }
    */
    
    
    
    /**
     * Is v a vertex in this graph?
     */
    public abstract boolean hasVertex(int v);
    
    /**
     * Is v-w an edge in this graph?
     */
    public abstract boolean hasEdge(int v, int w);
    
    
    public abstract void removeVertex(int v);
    
    
    public abstract int InducedEdges_FAST(Collection<Integer> S_HashSet);
    
    public int InducedEdges_FAST(int[] S)
    {
        return InducedEdges_FAST(Graph.toHashSet(S));
    }
    
    public static Collection<Integer> toHashSet(int[] S)
    {
        Collection<Integer> v = new HashSet<Integer>();
        for(int i=1; i<S.length; i++)
        {
            if (S[i]==1)
            {
                v.add(i);
            }
        }
        return v;
    }






    /**
     * Add edge v-w to this graph (if it is not already an edge)
     * Multi-threaded version
     */
    public abstract void MTaddEdge(int v, int w);
    
    
    /**
     * Add vertex v to this graph (if it is not already a vertex)
     * Multi-threaded version
     */
    public abstract void MTaddVertex(int v);

    /**
     * Is v-w an edge in this graph?
     * Multi-threaded version
     */
    public abstract boolean MThasEdge(int v, int w);

    
    public int[] defaultOrder()
    {
        return new int[V+1];
    }
    
    public String GetFilename()
    {
        return filename;
    }
}
