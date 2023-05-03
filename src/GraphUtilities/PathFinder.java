package GraphUtilities;

import BasicDataStructure.*;

public class PathFinder {
     // prev[v] = previous vertex on shortest path from s to v
    // dist[v] = length of shortest path from s to v
    
    private ST<Integer, Integer>  prev = new ST<Integer, Integer>();
    private ST<Integer, Integer> dist = new ST<Integer, Integer>();

    // run BFS in graph G from given source vertex s
    public PathFinder(Graph G, int s) {

        // put source on the queue
        Queue q = new Queue();
        q.enqueue(s);
        dist.put(s, 0);
        
        // repeated remove next vertex v from queue and insert
        // all its neighbors, provided they haven't yet been visited
        while (!q.isEmpty()) {
            int v = q.dequeue();
            for (Integer w : G.adjacentTo(v)) {
                if (!dist.contains(w)) {
                    q.enqueue(w);
                    dist.put(w, 1 + dist.get(v));
                    prev.put(w, v);
                }
            }
        }
    }

    // is v reachable from the source s?
    public boolean isReachable(int v) {
        return dist.contains(v);
    }

    // return the length of the shortest path from v to s
    public int distanceTo(int v) {
        if (!dist.contains(v)) return Integer.MAX_VALUE;
        return dist.get(v);
    }
    
    public Iterable<Integer> getAllReachable()
    {
        return dist.keys();
    }



}
