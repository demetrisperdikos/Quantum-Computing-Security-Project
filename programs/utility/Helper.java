package utility;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Helper {

    private static final String mainDir = "/Users/ec2-user/Review_Testing/programs/";
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public static Boolean getServer() {
        return true;
    }

    public static String getDBUser() {
        return "root";
    }

    public static String getDBPass() {
        return "12345";
    }

    public static String getConPath() {
        // MySql
        if (getServer())
            return "jdbc:mysql://localhost:3306/";
        else
            return "null";
    }

    public static Connection getConnection() throws SQLException {
        if (getServer())
            return DriverManager.getConnection(getConPath(), getDBUser(), getDBPass());
        else
            return DriverManager.getConnection(getConPath());
    }


    public static String getTablePrefix() {
        if (getServer())
            return "tpch.";
        else
            return "";
    }


    public static String getMainDir() {
        return mainDir;
    }

    public static Properties readPropertiesFile(String fileName) {
        FileInputStream fileInputStream;
        Properties properties = null;
        try {
            fileInputStream = new FileInputStream(fileName);
            properties = new Properties();
            properties.load(fileInputStream);
        } catch (IOException ioException) {
            log.log(Level.SEVERE, ioException.getMessage());
        }
        return properties;
    }

    public static int mod(int number) {
        int modulo = 100000007;
        number = number % modulo;
        if (number < 0)
            number = number + modulo;
        return number;
    }

    public static long mod(long number) {
        long modulo = 100000007;
        number = number % modulo;
        if (number < 0)
            number = number + modulo;
        return number;
    }

    public static int[] stringToIntArray(String data) {
        int[] result = new int[data.length()];
        for (int i = 0; i < data.length(); i++) {
            result[i] = (data.charAt(i) - '0');
        }
        return result;
    }

    public static void printResult(List<Integer> result, String fileName) throws IOException {
        System.out.println("The number of rows matching the query is " + result.size());

        FileWriter writer = new FileWriter(mainDir + "result/" + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        for (int data:result) {
            bufferedWriter.append(String.valueOf(data)).append(",");
        }
        bufferedWriter.close();
    }

    public static void printResult(Set<Integer> result, String fileName) throws IOException {
        System.out.println("The number of rows matching the query is " + result.size());

        FileWriter writer = new FileWriter(mainDir + "result/" + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        for (int data:result) {
            bufferedWriter.append(String.valueOf(data)).append(",");
        }
        bufferedWriter.close();
    }

    public static void printResult(int[][] result, int[] query, String fileName) throws IOException {

        FileWriter writer = new FileWriter(mainDir + "result/" + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        for (int i = 0; i < result.length; i++) {
            bufferedWriter.append(String.valueOf(query[i] + 1)).append("\n");
            for (int j = 0; j < result[0].length; j++) {
                bufferedWriter.append(String.valueOf(result[i][j])).append(",");
            }
            bufferedWriter.append("\n");
        }
        bufferedWriter.close();
    }

    public static ArrayList<Long> getProgramTimes(ArrayList<Instant> timestamps) {

        ArrayList<Long> durations = new ArrayList<>();

        for (int i = 0; i < timestamps.size() - 1; i++) {
            durations.add(Duration.between(timestamps.get(i), timestamps.get(i + 1)).toMillis());
        }

        return durations;
    }

    public static String strArrToStr(String[] arr) {
        ArrayList<String> arrAsList = new ArrayList<>(Arrays.asList(arr));
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    public static String arrToStr(int[] arr) {
        ArrayList<Integer> arrAsList = new ArrayList<>();
        for (Integer num : arr)
            arrAsList.add(num);
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    public static String arrToStr(int[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    public static int[][] strToStrArr1(String data) {
        String[] temp = data.split("\n");
        int[][] result = new int[temp.length][];

        int count = 0;
        for (String line : temp) {
            result[count++] = Stream.of(line.split(", "))
                    .mapToInt(Integer::parseInt)
                    .toArray();
        }
        return result;
    }

    public static int[] strToArr(String str) {
        ArrayList<Integer> arrList = new ArrayList<>();
        String temp[];

        if (str.contains(", "))
            temp = str.split(", ");

        else if (str.contains("|"))
            temp = str.split("\\|");

        else
            temp = new String[]{str};


        for (String val : temp) {
            arrList.add(Integer.parseInt(val));
        }
        int[] result = new int[arrList.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = arrList.get(i);
        }

        return result;
    }

    public static String convertMillisecondsToHourMinuteAndSeconds(long milliseconds) {
        long seconds = (milliseconds / 1000) % 60;
        long minutes = (milliseconds / (1000 * 60)) % 60;
        long hours = (milliseconds / (1000 * 60 * 60)) % 24;
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    public static void progressBar(double percentInp, long timeSinceStart) {
        int percent = (int) (percentInp + 0.5);
        if (percent == 0)
            percent = 1;

        String bar = "|";
        String progress = "";
        for (int i = 0; i < percent / 2 - 1; i++)
            progress += "=";
        if (percent == 100)
            progress += "=";
        else
            progress += ">";
        for (int i = 0; i < 50 - percent / 2; i++)
            progress += "-";
        String finalString = bar + progress + bar + " " + percent + "%  |  Est. Time Remaining: " + convertMillisecondsToHourMinuteAndSeconds(timeSinceStart * (100 - percent) / percent) + "        ";
        if (percent != 100)
            finalString += " \r";
        else
            finalString += " \n";
        System.out.print(finalString);
    }


    public static String arrToStr(long[] arr) {
        ArrayList<Long> arrAsList = new ArrayList<>();
        for (Long num : arr)
            arrAsList.add(num);
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }


    public static String arrToStr(String[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    public static String arrToStr(long[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    public static <T> String listToStr(ArrayList<T> list) {
        return list.stream().map(Object::toString).collect(Collectors.joining(", "));
    }


    public static long[] strToArr1(String str) {
        ArrayList<Long> arrList = new ArrayList<>();
        String temp[];

        if (str.contains(", "))
            temp = str.split(", ");

        else if (str.contains("|"))
            temp = str.split("\\|");

        else
            temp = new String[]{str};


        for (String val : temp) {
            arrList.add(Long.parseLong(val));
        }
        long[] result = new long[arrList.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = arrList.get(i);
        }

        return result;
    }

    public static int[][] strToArr(ArrayList<String> list, int startRow, int endRow) {
        int numValsInRow = Helper.strToArr(list.get(startRow)).length;

        int[][] result = new int[endRow - startRow][numValsInRow];

        for (int i = startRow; i < endRow; i++) {
            int[] arr = Helper.strToArr(list.get(i));
            System.arraycopy(arr, 0, result[i - startRow], 0, numValsInRow);
        }

        return result;
    }

    public static long[][] strToArr1(ArrayList<String> list, int startRow, int endRow) {
        int numValsInRow = Helper.strToArr1(list.get(startRow)).length;

        long[][] result = new long[endRow - startRow][numValsInRow];

        for (int i = startRow; i < endRow; i++) {
            long[] arr = Helper.strToArr1(list.get(i));
            System.arraycopy(arr, 0, result[i - startRow], 0, numValsInRow);
        }

        return result;
    }

    public static String[] strToStrArr(String str) {
        String result[] = str.split(", ");
        return result;
    }
}