package src._02_oneColumnNumberSearch.server;

import constant.*;
import utility.Helper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server1 {

    private static final String query_base1 = "select ";
    private static final String query_base2 = " from " + Helper.getTablePrefix() + "SERVERTABLE1 where rowID > ";

    private static int numRows;
    private static int numThreads;
    private static int numRowsPerThread;

    private static int additiveShare1;
    private static int seedServer;
    private static int seedClient;
    private static String columnName;

    private static int[] result;
    private static HashMap<Integer, Long> hashMap = new HashMap<>();

    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    private static int serverPort;
    private static int combinerPort;
    private static String combinerIP;

    private static class ParallelTask implements Runnable {

        private final int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            Connection con = null;
            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {
                String query = query_base1 + columnName + query_base2 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                Random randSeedServer = new Random(seedServer);
                Random randSeedClient = new Random(seedClient);

                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    result[i] = (int) Helper.mod(Helper.mod((rs.getLong(columnName) - additiveShare1) *
                            randSeedServer.nextInt(Constants.getRandomBound())) + randSeedClient.nextInt(Constants.getRandomBound()));
                }
            } catch (SQLException ex) {log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    private static void doWork(String[] data) {

        columnName = data[0];
        additiveShare1 = Integer.parseInt(data[1]);
        seedClient = Integer.parseInt(data[2]);
        result = new int[numRows];

        // The list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // Create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask(threadNum), "Thread" + threadNum));
        }

        // Start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // Wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    static class SocketCreation {

        private final Socket clientSocket;


        SocketCreation(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            ObjectInputStream inFromClient;
            Socket combinerSocket;
            ObjectOutputStream outToCombiner;
            String[] dataReceived;

            try {
                //Reading the data sent by Client
                inFromClient = new ObjectInputStream(clientSocket.getInputStream());
                dataReceived = (String[]) inFromClient.readObject();
                doWork(dataReceived);

                //Sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP, combinerPort);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result);
                combinerSocket.close();

                //Calculating timestamps
                timestamps.add(Instant.now());
                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server1 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    private void startServer() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort);
            System.out.println("Server1 Listening........");

            do {
                socket = ss.accept();
                timestamps = new ArrayList<>();
                timestamps.add(Instant.now());
                new SocketCreation(socket).run();
            } while (true);
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    private static void doPreWork() {

        String pathName = "config/Server1.properties";
        Properties properties = Helper.readPropertiesFile(Helper.getMainDir() + pathName);

        seedServer = Integer.parseInt(properties.getProperty("seedServer"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort"));
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));
        combinerIP = properties.getProperty("combinerIP");
    }

    public static void main(String[] args) throws IOException {

        doPreWork();

        Server1 server1 = new Server1();
        server1.startServer();

    }
}


