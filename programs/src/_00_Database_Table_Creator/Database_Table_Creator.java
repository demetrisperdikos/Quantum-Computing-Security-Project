package src._00_Database_Table_Creator;

import utility.*;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class Database_Table_Creator {


    private static final String query_base = "select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER from tpch.LINEITEM LIMIT ";
    private static int totalRows;
    private static int numThreads;
    private static int numRowsPerThread;
    private static boolean showDetails = false;
    private static final ArrayList<Long> threadTimes = new ArrayList<>();
    private static Connection con;
    static List<String[]> dataLines = new ArrayList<>();

    private static FileWriter writer1;
    private static FileWriter writer2;
    private static FileWriter writer3;
    private static FileWriter writer4;

    private static int origORD;
    private static int origPART;
    private static int origSUPP;
    private static int origLINE;



    public Database_Table_Creator(final int totalRows, final int numThreads, final boolean showDetails) {
            Database_Table_Creator.totalRows = totalRows;
            Database_Table_Creator.numThreads = numThreads;
            Database_Table_Creator.numRowsPerThread = totalRows/numThreads;
            Database_Table_Creator.showDetails = showDetails;
    }

    public static void main(String[] args) throws IOException {

        Database_Table_Creator DBShareCreation = new Database_Table_Creator((int) (Double.parseDouble(args[0]) * 1000000), 1, false);

        // doPrework connects to the database, deletes the existing SHARE tables, and creates two new SHARE tables
        doPreWork();

        // doWork lcreates and starts all threads
        doWork();

        // Write to file
        doPostWork();
    }

    private static void doPreWork() {

        System.out.println("Connection Started");

        try {
            Database_Table_Creator.con = Helper.getConnection();
        } catch (SQLException ex) {
            System.out.println(ex);
        }

        System.out.println("Preparing CSV Files");

        String diskPath = Helper.getMainDir() + "data/CSV_DATA/";

        try {
            writer1 = new FileWriter(diskPath + "ServerTable1.csv");
            writer2 = new FileWriter(diskPath + "ServerTable2.csv");
            writer3 = new FileWriter(diskPath + "ServerTable3.csv");
            writer4 = new FileWriter(diskPath + "ServerTable4.csv");
        } catch (IOException ex) {
            Logger.getLogger(Database_Table_Creator.class.getName()).log(Level.SEVERE, null, ex);
        }


    }

    private static long doWork() {

        List<Thread> threadList = new ArrayList<>(); // The list containing all the threads

        long avgOperationTime = 0;

        // Create threads and add them to threadlist

        for(int i = 0; i < numThreads; i++){
            int threadNum = i+1;
            threadList.add(new Thread(new ParallelTask(numRowsPerThread, threadNum), "Thread" + threadNum));
        }

        // Start all threads

        for(int i = 0; i < numThreads; i++){
            threadList.get(i).start();
        }

        // Wait for all threads to finish

        for (Thread thread : threadList) {
            try{
                thread.join();
            }catch(InterruptedException ex){
                System.out.println(ex.getMessage());
            }
        }

        // The Thread run times for each thread are stored in the array threadTimes
        // This loop calculates the average thread time across all the threads

        for (int i = 0; i < threadTimes.size(); i++) {
            avgOperationTime += threadTimes.get(i);
        }


        return(avgOperationTime/numThreads);

    }

    private static class ParallelTask implements Runnable {

        private final int numRows;
        private final int threadNum;

        public ParallelTask(final int numRows, final int threadNum) {
            this.numRows = numRows;
            this.threadNum = threadNum;
        }

        @Override
        public void run() {

            Instant threadStartTime = Instant.now();

            int count = 0;

            int startRow = (threadNum - 1) * numRows;

            Random rand = new Random();

            ResultSet rs;

            Instant startTime = Instant.now();


            try{
                System.out.println("Table Loading Started");
                Instant tableLoad = Instant.now();

                String query = query_base + startRow + ", " + numRows;
                Statement stmt = con.createStatement();
                rs = stmt.executeQuery(query);

                long tableLoadTime = Duration.between(tableLoad, Instant.now()).toMillis();

                System.out.println("Splitting Started");
                Instant splittingStart = Instant.now();

                for(int i = startRow; i < startRow + numRows; i++){
                    rs.next();

                    origORD = rs.getInt("L_ORDERKEY");
                    origPART= rs.getInt("L_PARTKEY");
                    origSUPP = rs.getInt("L_SUPPKEY");
                    origLINE = rs.getInt("L_LINENUMBER");



                    // MASTER_SUPPKEY
                    LinkedList<Integer> stack = new LinkedList<>();
                    ArrayList<Integer> numInArrList = new ArrayList<>();

                    ArrayList<Integer> c3p1LIST = new ArrayList<>();
                    ArrayList<Integer> c3p2LIST = new ArrayList<>();

                    int tempSUPP = origSUPP;

                    while (tempSUPP > 0) {
                        stack.push(tempSUPP % 10 );
                        tempSUPP = tempSUPP / 10;
                    }
                    while (!stack.isEmpty())
                        numInArrList.add(stack.pop());

                    for (Integer numPart : numInArrList)
                        c3p1LIST.add(rand.nextInt(100));

                    for (int j = 0; j < c3p1LIST.size(); j++)
                        c3p2LIST.add(numInArrList.get(j) - c3p1LIST.get(j));

                    String c01p1 = c3p1LIST.stream().map(Object::toString).collect(Collectors.joining("|"));
                    String c01p2 = c3p2LIST.stream().map(Object::toString).collect(Collectors.joining("|"));
                    String c01p3 = c01p1;
                    String c01p4 = c01p2;


                    // SPLIT_ORDERKEY
                    int c02p1 = Helper.mod(rand.nextInt(1000000));
                    int c02p2 = origORD - c02p1;
                    int c02p3 = c02p1;
                    int c02p4 = c02p2;


                    // SPLIT_PARTKEY
                    int c03p1 = Helper.mod(rand.nextInt(1000000));
                    int c03p2 = origPART - c03p1;
                    int c03p3 = c03p1;
                    int c03p4 = c03p2;


                    // SPLIT_LINENUMBER
                    int c04p1 = Helper.mod(rand.nextInt(1000000));
                    int c04p2 = origLINE - c04p1;
                    int c04p3 = c04p1;
                    int c04p4 = c04p2;




//                    // SHARE_ORDERKEY
//                    int temp1 = Helper.mod(origORD + Helper.PseudoRAND(origORD));
//                    int c05p1 = rand.nextInt(1000000);
//                    int c05p2 = temp1 - c05p1;
//                    int c05p3 = c05p1;
//                    int c05p4 = c05p2;
//
//
//                    // SHARE_PARTKEY
//                    int temp2 = Helper.mod(origPART + Helper.PseudoRAND(origPART));
//                    int c06p1 = rand.nextInt(1000000);
//                    int c06p2 = temp2 - c06p1;
//                    int c06p3 = c06p1;
//                    int c06p4 = c06p2;
//
//
//                    // SHARE_LINENUMBER
//                    int temp3 = Helper.mod(origLINE + Helper.PseudoRAND(origLINE));
//                    int c07p1 = rand.nextInt(1000000);
//                    int c07p2 = temp3- c07p1;
//                    int c07p3 = c07p1;
//                    int c07p4 = c07p2;




                    // M_ORDERKEY
                    int ordVals[] = func(origORD);
                    int c08p1 = ordVals[0];
                    int c08p2 = ordVals[1];
                    int c08p3 = ordVals[2];
                    int c08p4 = ordVals[3];


                    // M_PARTKEY
                    int partVals[] = func(origPART);
                    int c09p1 = partVals[0];
                    int c09p2 = partVals[1];
                    int c09p3 = partVals[2];
                    int c09p4 = partVals[3];


                    // M_LINENUMBER
                    int lineVals[] = func(origLINE);
                    int c10p1 = lineVals[0];
                    int c10p2 = lineVals[1];
                    int c10p3 = lineVals[2];
                    int c10p4 = lineVals[3];

                    // M_LINENUMBER
                    int suppVals[] = func(origSUPP);
                    int c11p1 = suppVals[0];
                    int c11p2 = suppVals[1];
                    int c11p3 = suppVals[2];
                    int c11p4 = suppVals[3];


                    try {

                        writer1.append(c01p1);
                        writer1.append(",");
                        writer1.append(String.valueOf(c02p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c03p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c04p1));
                        writer1.append(",");
//                        writer1.append(String.valueOf(c05p1));
//                        writer1.append(",");
//                        writer1.append(String.valueOf(c06p1));
//                        writer1.append(",");
//                        writer1.append(String.valueOf(c07p1));
//                        writer1.append(",");
                        writer1.append(String.valueOf(c08p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c09p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c10p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c11p1));
                        writer1.append("\n");

                        writer2.append(c01p2);
                        writer2.append(",");
                        writer2.append(String.valueOf(c02p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c03p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c04p2));
                        writer2.append(",");
//                        writer2.append(String.valueOf(c05p2));
//                        writer2.append(",");
//                        writer2.append(String.valueOf(c06p2));
//                        writer2.append(",");
//                        writer2.append(String.valueOf(c07p2));
//                        writer2.append(",");
                        writer2.append(String.valueOf(c08p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c09p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c10p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c11p2));
                        writer2.append("\n");

                        writer3.append(c01p3);
                        writer3.append(",");
                        writer3.append(String.valueOf(c02p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c03p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c04p3));
                        writer3.append(",");
//                        writer3.append(String.valueOf(c05p3));
//                        writer3.append(",");
//                        writer3.append(String.valueOf(c06p3));
//                        writer3.append(",");
//                        writer3.append(String.valueOf(c07p3));
//                        writer3.append(",");
                        writer3.append(String.valueOf(c08p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c09p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c10p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c11p3));
                        writer3.append("\n");

                        writer4.append(c01p4);
                        writer4.append(",");
                        writer4.append(String.valueOf(c02p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c03p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c04p4));
                        writer4.append(",");
//                        writer4.append(String.valueOf(c05p4));
//                        writer4.append(",");
//                        writer4.append(String.valueOf(c06p4));
//                        writer4.append(",");
//                        writer4.append(String.valueOf(c07p4));
//                        writer4.append(",");
                        writer4.append(String.valueOf(c08p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c09p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c10p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c11p4));
                        writer4.append("\n");

                    } catch (IOException ex) {
                        Logger.getLogger(Database_Table_Creator.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    if(i%(totalRows/100) == 0){
                        double percent = 100 * ((double)i/(double)totalRows);
                        Helper.progressBar(percent, Duration.between(startTime, Instant.now()).toMillis());
                    }
                }

                long tableSplittingTime = Duration.between(splittingStart, Instant.now()).toMillis();

                System.out.println("100% complete");

            System.out.println();
            System.out.println("Table Loading Time: " + tableLoadTime + " ms");
            System.out.println("Table Splitting Time: " + tableSplittingTime + " ms");


            } catch (SQLException ex) {
                System.out.println(ex);
            }

            Duration totalThreadTime = Duration.between(threadStartTime, Instant.now());

            if(showDetails) {
                System.out.println("\n" + Thread.currentThread().getName().toUpperCase());
                System.out.println("Total Operations: " + count);
                System.out.println("Total Thread Time: " + totalThreadTime.toMillis() + " ms");
                System.out.println("Thread Start Time: " + DateTimeFormatter.ofPattern("hh:mm:ss.SSS").format(LocalDateTime.ofInstant(threadStartTime, ZoneOffset.UTC)));
                System.out.println("Thread Start Time: " + DateTimeFormatter.ofPattern("hh:mm:ss.SSS").format(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)));
            }

            threadTimes.add(totalThreadTime.toMillis());
        }
    }

    private static void doPostWork(){
        try {
            writer1.toString();
            writer1.flush();
            writer1.close();

            writer2.toString();
            writer2.flush();
            writer2.close();

            writer3.toString();
            writer3.flush();
            writer3.close();

            writer4.toString();
            writer4.flush();
            writer4.close();
        } catch (IOException ex) {
            Logger.getLogger(Database_Table_Creator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static int[] func(int b){
        Random rand = new Random();

        int m = rand.nextInt(2000) - 1000;

        int m1 = 1 * m + b;
        int m2 = 2 * m + b;
        int m3 = 3 * m + b;
        int m4 = 4 * m + b;

        int vals[] = new int[]{Helper.mod(m1), Helper.mod(m2), Helper.mod(m3), Helper.mod(m4)};

        return vals;
    }
}




