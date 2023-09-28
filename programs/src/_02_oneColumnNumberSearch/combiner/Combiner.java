package src._02_oneColumnNumberSearch.combiner;

import constant.*;
import utility.Helper;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Combiner extends Thread {
    private static List<int[]> serverResult = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static int[] result;

    private static int numRows;
    private static int numThreads;
    private static int numRowsPerThread;

    private static int combinerPort;
    private static int clientPort;
    private static String clientIP;

    private static int[] server1;
    private static int[] server2;

    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    private static class ParallelTask implements Runnable {
        private int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;
            for (int i = startRow; i < endRow; i++) {
                result[i] = (int) Helper.mod((long) server1[i] + (long) server2[i]);
            }
        }
    }

    private static void doWork() {
        // The list containing all the threads

        server1 = serverResult.get(0);
        server2 = serverResult.get(1);
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

    class SocketCreation implements Runnable {

        private final Socket serverSocket;

        SocketCreation(Socket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            ObjectInputStream inFromServer;
            try {
                inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                serverResult.add((int[]) inFromServer.readObject());
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    @Override
    public void run() {
        startCombiner();
        super.run();
    }

    private void startCombiner() {
        Socket serverSocket;
        Socket clientSocket;
        ArrayList<Future> serverJobs = new ArrayList<>();

        try {
            ServerSocket ss = new ServerSocket(combinerPort);
            System.out.println("Combiner Listening........");

            while (true) {
                //Reading data from the server
                serverSocket = ss.accept();
                socketCreations.add(new SocketCreation(serverSocket));

                //Processing data received from both the servers
                if (socketCreations.size() == 2) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();
                    doWork();
                    //Sending data from the client
                    clientSocket = new Socket(clientIP, clientPort);
                    ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                    outToClient.writeObject(result);
                    clientSocket.close();
                    //Resetting storage variables
                    result = new int[numRows];
                    serverJobs = new ArrayList<>();
                    serverResult = Collections.synchronizedList(new ArrayList<>());
                    socketCreations = new ArrayList<>();

                    //Calculating the time spent
                    timestamps.add(Instant.now());
                    System.out.println(Helper.getProgramTimes(timestamps));
//                    log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                }
            }
        } catch (IOException | ExecutionException | InterruptedException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    private static void doPreWork(String[] args) {
        //Reading Combiner property file
        String pathName = "config/Combiner.properties";
        Properties properties = Helper.readPropertiesFile(Helper.getMainDir() + pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        clientIP = properties.getProperty("clientIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));

        result = new int[numRows];
    }

    public static void main(String args[]) {

        doPreWork(args);

        Combiner combiner = new Combiner();
        combiner.startCombiner();

    }
}