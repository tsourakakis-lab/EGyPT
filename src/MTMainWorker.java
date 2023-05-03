import GraphPartitioner.StreamingPartitioner;
import GraphUtilities.Graph;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.*;

 
public class MTMainWorker implements Runnable {
    Thread thread;
    int threadId;
    Graph G;
    int noClusters;
    int runType;
    BufferedWriter logFile;
    String logFileName;
    boolean createGiraphFiles = false;
  
            
    public MTMainWorker(int id, Graph Gr, int noclus, int type, BufferedWriter log, String filename, boolean createpart)
    {
        thread = new Thread(this, "WorkerThread_" + ((Integer)id).toString());
        threadId = id;
        G = Gr;
        noClusters = noclus;
        runType = type;
        logFile = log;
        logFileName = filename;
        createGiraphFiles = createpart;
        
        thread.start();
        
        
    }
    
    
    

    
    public static String getAlgoName(int type)
    {
        String algoName = "Unknown";
        
        switch (type)
        {
            case 1:
            {
                algoName = "Stanton";
            }
            break;

            case 2:
            {
                algoName = "NNCN15";
            }
            break;

            case 3:
            {
                algoName = "NNCN15K15";
            }
            break;

            case 4:
            {
                algoName = "NNC15K15";
            }
            break;

            case 5:
            {
                algoName = "NNCN15K11";
            }
            break;
                
            case 6:
            {
                algoName = "NNCN125";
            }
            break;
                
            case 7:
            {
                algoName = "NNCN125K12";
            }
            break;
                
            case 8:
            {
                algoName = "NNCN175";
            }
            break;
                
            case 9:
            {
                algoName = "NNCN175K12";
            }
            break;
                
            case 10:
            {
                algoName = "NNCN2";
            }
            break;
                
            case 11:
            {
                algoName = "NNCN2K12";
            }
            break;
                
            case 12:
            {
                algoName = "NNCN15K11_Edges";
            }
            break;
                
            case 13:
            {
                algoName = "NNCN15K2000_Edges";
            }
            break;
                
            case 14:
            {
                algoName = "Random";
            }
            break;
                
            case 15:
            {
                algoName = "NNC15K11_Edges";
            }
            break;

            case 16:
            {
                algoName = "NNC15K2000_Edges";
            }
            break;
                
            case 17:
            {
                algoName = "Stanton_Edges";
            }
            break;
        }
        
        return algoName;
    }
    
    public void run() 
    {
        double starttime;
        double total;
        StreamingPartitioner sp = new StreamingPartitioner(G,noClusters);
        String filename = logFileName;
        String outFileName;
        String algoName = null;

        try{
            switch (runType)
            {
                case 1:
                {
                    sp.LinearWeightedDegreePartitioning();
                    sp.printLineStatistics(logFile, runType, 2);
                }
                break;
                    
                case 2:
                {
                    sp.NonNeighborsConvexfNormalized(1.5);
                    //sp.Fast_NonNeighborsConvexfNormalized(1.5);
                    sp.printLineStatistics(logFile, runType, 4);
                }
                break;

                case 3:
                {
                    sp.NonNeighborsConvexfNormalizedKnee(1.5, 1.5);
                    sp.printLineStatistics(logFile, runType, 6);
                }
                break;

                case 4:
                {
                    sp.NonNeighborsConvexfKnee(1.5, 1.5);
                    sp.printLineStatistics(logFile, runType, 5);
                }
                break;

                case 5:
                {
                    sp.NonNeighborsConvexfNormalizedKnee(1.5, 1.1);
                    sp.printLineStatistics(logFile, runType, 6);
                }
                break;
                    
                case 6:
                {
                    sp.NonNeighborsConvexfNormalized(1.25);
                    sp.printLineStatistics(logFile, runType, 4);
                }
                break;
                    
                case 7:
                {
                    sp.NonNeighborsConvexfNormalizedKnee(1.25, 1.2);
                    sp.printLineStatistics(logFile, runType, 6);
                }
                break;
                    
                case 8:
                {
                    sp.NonNeighborsConvexfNormalized(1.75);
                    sp.printLineStatistics(logFile, runType, 4);
                }
                break;
                    
                case 9:
                {
                    sp.NonNeighborsConvexfNormalizedKnee(1.75, 1.2);
                    sp.printLineStatistics(logFile, runType, 6);
                }
                break;
                    
                case 10:
                {
                    sp.NonNeighborsConvexfNormalized(2);
                    sp.printLineStatistics(logFile, runType, 4);
                }
                break;
                    
                case 11:
                {
                    sp.NonNeighborsConvexfNormalizedKnee(2, 1.2);
                    sp.printLineStatistics(logFile, runType, 6);
                }
                break;
                    
                case 12:
                {
                    sp.NonNeighborsConvexfNormalizedKneeEdges(1.5, 1.01);
                    sp.printLineStatistics(logFile, runType, 7);
                }
                break;
                    
                case 13:
                {
                    // Deliberately high knee to avoid it
                    sp.NonNeighborsConvexfNormalizedKneeEdges(2, 1.01);
                    sp.printLineStatistics(logFile, runType, 7);
                }
                break;
                    
                case 14:
                {
                    sp.HashingPartitioning();
                    sp.printLineStatistics(logFile, runType, 1);
                }
                break;
                    
                case 15:
                {
                    sp.NonNeighborsConvexfKneeEdges(1.5, 1.01);
                    sp.printLineStatistics(logFile, runType, 8);
                }
                break;

                case 16:
                {
                    sp.NonNeighborsConvexfKneeEdges(1.1, 1.01);
                    sp.printLineStatistics(logFile, runType, 8);
                }
                break;
                    
                case 17:
                {
                    sp.LinearWeightedDegreePartitioningEdges();
                    sp.printLineStatistics(logFile, runType, 9);
                }
                break;
                    
            }

            algoName = getAlgoName(runType);
            if (createGiraphFiles && algoName != null)
            {
                System.out.println(logFileName + "_m" + algoName + ": Dumping out the partition...");
                outFileName = filename.replace(".txt", "");
                sp.Partition2GiraphClustered(outFileName + "_m" + algoName, "txt");
            }
            
        }
        catch (IOException e)
        {

        }

        MTMain.DecreaseRunningThreads();
    }

}

