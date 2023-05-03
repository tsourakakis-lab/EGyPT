package GraphUtilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;


public class ArrayGraph extends Graph {
    
    // number of edges
    protected long E;
    
    static int MAX_VERTICES = 1000000000;
    // JavaVM fails if I use Integer.MAX_VALUE sizes
    static int MAX_EDGES_PER_ARRAY = Integer.MAX_VALUE/2;
    private int[] Degrees;
    //private ArrayList<int[]> Edges;
    private int[][] Edges;
    private int[] Vertices;
    private int[] VertexMapping;
    private int[] DefaultOrder;
    private long[] NeighboursStart;
    private long[] NeighboursEnd;
    
    /**
     * Create an empty graph with no vertices or edges.
     */
    public ArrayGraph() {
    }
    
    public ArrayGraph(String edgefile, String delimiter, boolean useHeader) throws IOException {
        this();
        Preprocess(edgefile, delimiter, useHeader);
        ImportGraph(edgefile, delimiter, useHeader);
    }
    
    public ArrayGraph(String edgefile, String delimiter, int noThreads, boolean useHeader) throws IOException {
        this();
        Preprocess(edgefile, delimiter, useHeader);
        ImportGraph(edgefile, delimiter, useHeader);  // no MT version
        
        /*
        if (noThreads == 0)
        {
            ImportGraph(edgefile, delimiter);
        }
        else
        {
            MTImportGraph(edgefile, delimiter, noThreads);
        }
        */
    }


    /*
    private void PreAddVertex(TreeMap<Integer, Integer> Vertices, Integer u)
    {
        if (Vertices.containsKey(u))
        {
            Integer value = Vertices.get(u);
            Vertices.put(u, value+1);
        }
        else
        {
            Vertices.put(u, 1);
        }
    }
    */
    

    protected void DEBUGLARGEPreprocess(String edgefile, String delimiter, boolean useHeader) throws IOException
    {
        // Init local structures
        initEdges(2000000000);
        V = 100000000;
        DefaultOrder = new int[V+1];
        NeighboursStart = new long[V+1];
        NeighboursEnd = new long[V+1];
        Degrees = new int[V+1];
        Vertices = new int[V+1];
        VertexMapping = new int[V+1];


        long start = 0;
        for(int v=1; v<=V; v++)
        {
            VertexMapping[v] = v;
            Vertices[v] = v;
            DefaultOrder[v] = v;
            NeighboursStart[v] = start;
            NeighboursEnd[v] = start;
            Degrees[v] = 10;
            start += Degrees[v];
        }

    }
        
    protected void Preprocess(String edgefile, String delimiter, boolean useHeader) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(edgefile));
        String line;
        String[] result;
        int noVertices = 0;
        int noEdges = 0;
        int maxV, maxDegree; 
        
        maxV = -1;
        maxDegree = -1;
        int min_vertice = MAX_VERTICES;
        int [] SparseDegrees = new int[MAX_VERTICES];
        for(int i=0; i<MAX_VERTICES; i++) SparseDegrees[i] = 0;
        
        // the first line of the file contains the number of vertices and then
        // the number of edges
        if (useHeader)
        {
            line = br.readLine();
            result = line.split(delimiter);
            noVertices = Integer.parseInt(result[0]);
            noEdges = Integer.parseInt(result[1]);        
            System.out.format("Expecting %d vertices and %d edges%n", noVertices, noEdges);
        }
       
        
        E = 0;
        line = br.readLine();
        long lastTime = System.currentTimeMillis();
        while( line!= null )
        {
            E++;
            result = line.split(delimiter);
            int u = Integer.parseInt(result[0]);
            int v = Integer.parseInt(result[1]);

            if (u < MAX_VERTICES && v < MAX_VERTICES)
            {
                SparseDegrees[u]++;
                SparseDegrees[v]++;
                maxV = Math.max(maxV, Math.max(u,v));
                min_vertice = Math.min(min_vertice, Math.min(u,v));
            }
            else
            {
                System.out.format("Edge (%d,%d) out of limit!%n", u, v);
            }
            
            line = br.readLine();
            
            // bozidar DEBUG
            if (E % 1000000 == 0)
            {
                long delta = System.currentTimeMillis() - lastTime;
                System.out.println("Preprocessing line " + E + " after " + delta);
                lastTime = System.currentTimeMillis();
            }
        }
        /*
        if( E != noEdges) {
            //DEBUGMSG("Check, #edges not equal to number of lines read");
            System.out.format("Check, #edges %d not equal to #lines read %d%n", E, noEdges);
        }
        */
        br.close();        
        

        if (min_vertice != 1) System.out.println("Vertices don't start from 1!");

        
        
        V = 1;
        boolean firstGap = true;
        Vertices = new int[maxV+1];
        VertexMapping = new int[maxV+1];
        
        // Map vertices to a continuous numbering
        for(int v=1; v<=maxV; v++)
        {
            if (SparseDegrees[v] > 0)
            {        
                Vertices[V] = v;
                VertexMapping[v] = V;
                V++;
            }
            else
            {                
                //System.out.println("Vertices have gaps in enumeration: " + v + " doesn't exist!");
                if (firstGap)
                {
                    System.out.println("Gap in vertex numbering detected");
                    firstGap = false;
                }
            }
        }

        V = V-1;

        
        // Init local structures
        initEdges(2*E);
        DefaultOrder = new int[V+1];
        NeighboursStart = new long[V+1];
        NeighboursEnd = new long[V+1];
        Degrees = new int[V+1];
        

        long start = 0;
        for(int v=1; v<=V; v++)
        {
            DefaultOrder[v] = v;
            NeighboursStart[v] = start;
            NeighboursEnd[v] = start;
            Degrees[v] = SparseDegrees[Vertices[v]];
            start += Degrees[v];
            maxDegree = Math.max(Degrees[v], maxDegree);
        }

        
        System.out.format("Total vertices: %d, highest vertex id: %d, total edges: %d, maxDegree: %d", V, maxV, E, maxDegree);
        if (useHeader)
        {
            System.out.format(", reported vertices: %d, reported edges: %d", noVertices, noEdges);
        }
        System.out.println();
    }
    
    
    /**
     * Return the set of neighbours of vertex v as in Iterable.
     */
    public Iterable<Integer> adjacentTo(int v) {
        //System.out.println("Should not be used: adjacentTo");
        ArrayList<Integer> neighborhood = new ArrayList();
        for(long i=NeighboursStart[v]; i<NeighboursEnd[v]; i++)
        {
            neighborhood.add(getEdge(i));
        }
        return neighborhood;
    }
    
    /**
     * Add edge v-w to this graph (if it is not already an edge)
     */
    public void addEdge(int vv, int vw) {
        int v = VertexMapping[vv], w = VertexMapping[vw];
        boolean exists = false;
        
        // Check if it exists
        // bozidar: disabled, as it takes very long time for some graphs (e.g. twitter)
        /*
        exists = false;
        long i=NeighboursStart[v];
        while (i<NeighboursEnd[v] && !exists)
        {
            exists = (getEdge(i) == w);
            i++;
        }
        */
        
        if(!exists)
        {
            addEdgeLong(NeighboursEnd[v], w);
            NeighboursEnd[v]++;
            assert(NeighboursEnd[v] <= E && (v == V || NeighboursEnd[v] <= NeighboursStart[v+1]));
            addEdgeLong(NeighboursEnd[w], v);
            NeighboursEnd[w]++;
            assert(NeighboursEnd[w] <= E && (w == V || NeighboursEnd[w] <= NeighboursStart[w+1]));
        }
    }
    
    /**
     * Add vertex v to this graph (if it is not already a vertex)
     */
    public void addVertex(int v) {
        System.out.append("Unsupported: addVertex");
    }

    
    
    
    /**
     * Degree of vertex v.
     */
    public int degree(int v) 
    {
        assert(v == V || (Degrees[v] == NeighboursEnd[v] - NeighboursStart[v]));
        return Degrees[v];
    }
    
    
    
    
    /**
     * This function returns the degree of vertex u in set S, which is indicated
     * by S_Hashset
     */   
    public int InducedDegree_FAST(Collection<Integer> S_HashSet,int u)
    {
        int deg = 0;
        
        assert(u <= V);
        
        for(long i=NeighboursStart[u]; i<NeighboursEnd[u]; i++)
        {
            int y = getEdge(i);
            if(u != y && S_HashSet.contains(y))
            {
                deg++;
            }
        }
        
        return deg;
        
    }
    

    
    
    
    public int InducedTriangles_FAST(Collection<Integer> S_HashSet)
    {
        System.out.append("Unsupported: InducedDegree_FAST2");
        return 0;
    }
    
    // to test
     public int InducedTriangles_FAST(Collection<Integer> S_HashSet,int v)
    {
        System.out.append("Unsupported: InducedDegree_FAST3");
        return 0;
    }
     
    
   
    /**
     * Return the set of vertices as an Iterable.
     */
    /*
    public Iterable<Integer> vertices() {
        return st;
    }
    */
    
    
    
    /**
     * Is v a vertex in this graph?
     */
    public boolean hasVertex(int v) {
        return v <= V;
    }
    
    /**
     * Is v-w an edge in this graph?
     */
    public boolean hasEdge(int v, int w) {
        boolean found1 = false, found2 = false;

        long i=NeighboursStart[v];
        while (i < NeighboursEnd[v] && getEdge(i) != w) i++;
        found1 = (i < NeighboursEnd[v] && getEdge(i) == w);

        i=NeighboursStart[w];
        while (i < NeighboursEnd[w] && getEdge(i) != v) i++;
        found2 = (i < NeighboursEnd[w] && getEdge(i) == v);
            
        assert(found1 == found2);
        return found1;
    }
    
    
    public void removeVertex(int vv)
    {
        System.out.append("Unsupported: removeVertex");
    }
    
    
    public int InducedEdges_FAST(Collection<Integer> S_HashSet)
    {
        System.out.append("Unsupported: InducedEdges_FAST");
        return 0;
    }
    



    /**
     * Add edge v-w to this graph (if it is not already an edge)
     * Multi-threaded version
     */
    public void MTaddEdge(int v, int w) 
    {
        System.out.append("Unsupported: MTaddEdge");
    }
    
    
    /**
     * Add vertex v to this graph (if it is not already a vertex)
     * Multi-threaded version
     */
    public void MTaddVertex(int v) 
    {
        System.out.append("Unsupported: MTaddVertex");
    }

    /**
     * Is v-w an edge in this graph?
     * Multi-threaded version
     */
    public boolean MThasEdge(int v, int w) {
        return hasEdge(v,w);
    }

    
    public int[] defaultOrder()
    {
        return DefaultOrder;
    }
    

    
    private void initEdges(long noEdges)
    {
        int noArrays = (int) (noEdges / (long)MAX_EDGES_PER_ARRAY) + 1;
        int lastArraySize = (int) noEdges;
        if (noArrays > 1)
        {
            lastArraySize = (int)(noEdges - (long)MAX_EDGES_PER_ARRAY * ((long)noArrays-1));
        }

        /*
        Edges = new ArrayList();
        for (int i=0; i<noArrays-1; i++)
        {
            System.out.println("Added array " + i);
            Edges.add(i, new int[MAX_EDGES_PER_ARRAY]);
        }
        Edges.add(noArrays-1, new int[lastArraySize]);
        */

        Edges = new int[noArrays][];
        for (int i=0; i<noArrays-1; i++)
        {
            System.out.println("Added array " + i);
            Edges[i] = new int[MAX_EDGES_PER_ARRAY];
        }

        Edges[noArrays-1] = new int[lastArraySize];
    }
    
    
    private void addEdgeLong(long index, int value)
    {
        int arrayIndex, elementIndex;
        arrayIndex = (int) (index / (long)MAX_EDGES_PER_ARRAY);
        elementIndex = (int) (index % (long)MAX_EDGES_PER_ARRAY);
        //int [] array = Edges.get(arrayIndex);
        Edges[arrayIndex][elementIndex] = value;
    }

    
    private int getEdge(long index)
    {
        int arrayIndex, elementIndex;
        arrayIndex = (int) (index / (long)MAX_EDGES_PER_ARRAY);
        elementIndex = (int) (index % (long)MAX_EDGES_PER_ARRAY);
        //int [] array = Edges.get(arrayIndex);
        return Edges[arrayIndex][elementIndex];
    }

    
    /**
     * Number of edges.
     */
    public long E() {
        //System.out.println("Unsupported - long!");
        return E;
    }

}
