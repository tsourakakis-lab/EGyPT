import java.io.IOException;
 
public class CmdArgs {
    public int [] noClusters = null;
    public int [] Types = null;
    public boolean createGiraphFiles = false;
    public boolean useHeader = false;
    public String fileName = null;
    
    
    public CmdArgs(String [] args) throws IOException
    {
        try
        {
            for(String arg : args)
            {
                String [] values = arg.split("=");
                
                switch(values[0].toLowerCase())
                {
                    case "-filename":
                        fileName = values[1];
                        break;

                    case "-noclusters":
                        String [] strClusters = values[1].split(",");
                        noClusters = new int[strClusters.length];
                        for(int i=0; i<strClusters.length; i++)
                        {
                            noClusters[i] = Integer.parseInt(strClusters[i]);
                        }
                        break;

                    case "-types":
                        String [] strTypes = values[1].split(",");
                        Types = new int[strTypes.length];
                        for(int i=0; i<strTypes.length; i++)
                        {
                            Types[i] = Integer.parseInt(strTypes[i]);
                        }
                        break;
                        
                    case "-creategiraphfile":
                        createGiraphFiles = ((values[1].toLowerCase()).equals("true") );
                        break;
                        
                    case "-useheader":
                        useHeader = ((values[1].toLowerCase()).equals("true") );
                        break;
                }
            }
            
            if (fileName == null || noClusters == null || Types == null )
            {
                throw new Exception();
            }
        }
        catch (Exception e)
        {
            System.out.println("Usage: MTMain -filename=<filename> -noClusters=<noClusters1>,...,<noClustersN>");
            System.out.println("       -types=<type1>,...,<typeN> [-createGiraphFiles=true|false] [-useHeader=true|false]");
            System.out.println("");
            System.out.println("-filename:          filename of the graph to be partitioned.");
            System.out.println("-noClusters:        partitioning the graph in multiple number of clusters, in parallel.");
            System.out.println("                    Cluster numbers are comma separated.");
            System.out.println("-types:             integer types (1-17) of partitioning algorithms to run on the graph.");
            System.out.println("                    Multiple partitioning can be run in parallel, ");
            System.out.println("                    in which case the types are comma separated. ");
            System.out.println("                    List of algorithms is given below.");
            System.out.println("-createGiraphFiles: if true dumps out partitions to files that can be used in Giraph.");
            System.out.println("-useHeader:         if true, skips the first line which is treated as a header.");
            System.out.println("");
            System.out.println("Graph file contain edges. The format of a line is: <vertex1> <vertex2>");
            System.out.println("");
            System.out.println("The list of supported algorithms types is:");
            for (int i = 1; i<=17; i++)
            {
                System.out.println("    " + i + " - " + MTMainWorker.getAlgoName(i));
            }
            throw new IOException(); 
        }
               
    }
    
}
