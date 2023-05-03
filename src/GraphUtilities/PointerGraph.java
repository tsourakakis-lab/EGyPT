/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author C. E. Tsourakakis
 */
public class PointerGraph extends Graph{
    
    // symbol table: key = string vertex, value = set of neighboring vertices
    public ST<Integer, SET<Integer>> st;
    
    
    /**
     * Create an empty graph with no vertices or edges.
     */
    public PointerGraph() {
        st = new ST<Integer, SET<Integer>>();
    }
    
    public PointerGraph(String edgefile, String delimiter) throws IOException {
        this();
        ImportGraph(edgefile, delimiter);
    }
    
    public PointerGraph(String edgefile, String delimiter, int noThreads) throws IOException {
        this();
        if (noThreads == 0)
        {
            ImportGraph(edgefile, delimiter);
        }
        else
        {
            MTImportGraph(edgefile, delimiter, noThreads);
        }
        V = st.size();
    }
   
    /**
     * Return the set of neighbours of vertex v as in Iterable.
     */
    public Iterable<Integer> adjacentTo(int v) {
        // return empty set if vertex isn't in graph
        if (!hasVertex(v)) return new SET<Integer>();
        else               return st.get(v);
    }
    
    /**
     * Add edge v-w to this graph (if it is not already an edge)
     */
    public void addEdge(int v, int w) {
        if (!hasEdge(v, w)) {
            E++;
        }
        if (!hasVertex(v)) {
            addVertex(v);
        }
        if (!hasVertex(w)) {
            addVertex(w);
        }
        st.get(v).add(w);
        st.get(w).add(v);
    }
    
    /**
     * Add vertex v to this graph (if it is not already a vertex)
     */
    public void addVertex(int v) {
        if (!hasVertex(v)) {
            //System.out.println("adding vertex "+v);
            st.put(v, new SET<Integer>());
        }
    }

    
    
    
    /**
     * Degree of vertex v.
     */
    public int degree(int v) {
        
        if (!st.contains(v)) 
            throw new RuntimeException(v + " is not a vertex");
        else 
            return st.get(v).size();
    }
    
    
    
    
    /**
     * This function returns the degree of vertex u in set S, which is indicated
     * by S_Hashset
     */   
    public int InducedDegree_FAST(Collection<Integer> S_HashSet,int u)
    {
        int deg = 0;
        if(!st.contains(u))
        {
            throw new RuntimeException(u + " is not a vertex");
        }
        
        
        for(int y:adjacentTo(u))
        {
            if(u != y && S_HashSet.contains(y))
            {
                deg++;
            }
        }
        
        return deg;
    }
    

    
    
    
    public int InducedTriangles_FAST(Collection<Integer> S_HashSet)
    {
        int triangles = 0;
        
        for(int x:S_HashSet)
        {
            SET<Integer> neighbors = this.st.get(x);
            for(int y:neighbors)
            {
                if (S_HashSet.contains(y))
                {
                    SET<Integer> neighborsY = this.st.get(y);
                    for (int z:neighborsY)
                    {
                        if (z != x && S_HashSet.contains(z) && neighbors.contains(z))
                        {
                            triangles++;
                        }
                    }
                }
            }
        }
        
        return triangles/6;
    }
    
    // to test
     public int InducedTriangles_FAST(Collection<Integer> S_HashSet,int v)
    {
        int triangles = 0;
        SET<Integer> neighbors = this.st.get(v);
        for(int y:neighbors)
        {
            if (S_HashSet.contains(y))
            {
                SET<Integer> neighborsY = this.st.get(y);
                for (int z:neighborsY)
                {
                    if (z != v && S_HashSet.contains(z) && neighbors.contains(z))
                    {
                        triangles++;
                    }
                }
            }
        }
        return triangles/2;
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
        return st.contains(v);
    }
    
    /**
     * Is v-w an edge in this graph?
     */
    public boolean hasEdge(int v, int w) {
        if (!hasVertex(v)) return false;
        return st.get(v).contains(w);
    }
    
    
    public void removeVertex(int v)
    {
        if (!hasVertex(v)) return;
        int deg = degree(v);
        for( int i: st.get(v) )
            st.get(i).delete(v);
        //st.delete(v);

    }
    
    
    public int InducedEdges_FAST(Collection<Integer> S_HashSet)
    {
        int inducedEdges = 0;
        for(int i:S_HashSet)
        {
            SET<Integer> neighbors = this.st.get(i);
            if (neighbors != null)
            {
                for(int v:neighbors)
                {
                    if(v!=i && S_HashSet.contains(v))
                    {
                        inducedEdges++;
                    }
                }
            }
        }
        inducedEdges /= 2;
        
        return inducedEdges;
    }
    



    /**
     * Add edge v-w to this graph (if it is not already an edge)
     * Multi-threaded version
     */
    public void MTaddEdge(int v, int w) {
        if (!MThasEdge(v, w)) {
            // bozidar: This is a dirty hack:
            // we should sync around E but it is not an object - to be changed
            synchronized(this)
            {
                E++;
            }
        }
        MTaddVertex(v);
        MTaddVertex(w);

        // If another thread is adding the vertice, 
        // wait until the neighbourhood is added and only then proceed
        SET<Integer> neighbourhood = null;
        while(neighbourhood == null) neighbourhood = st.get(v);
        synchronized(neighbourhood)
        {
            neighbourhood.add(w);
        }
        neighbourhood = null;
        while(neighbourhood == null) neighbourhood = st.get(w);
        synchronized(neighbourhood)
        {
            neighbourhood.add(v);
        }

    }
    
    
    /**
     * Add vertex v to this graph (if it is not already a vertex)
     * Multi-threaded version
     */
    public void MTaddVertex(int v) {
        if (!hasVertex(v)) 
        {
            synchronized(st)
            {
                if (!hasVertex(v)) {
                    st.put(v, new SET<Integer>());
                }
            }
        }
    }

    /**
     * Is v-w an edge in this graph?
     * Multi-threaded version
     */
    public boolean MThasEdge(int v, int w) {
        if (!hasVertex(v)) return false;
        SET<Integer> neighbourhood;
        neighbourhood = st.get(v);
        if (neighbourhood == null) return false;
        return neighbourhood.contains(w);
    }

    
    public int[] defaultOrder()
    {
        int[] array = new int[V+1];
        for(int i=0; i<=V; i++) array[i] = i;
        return array;
    }
    
}
