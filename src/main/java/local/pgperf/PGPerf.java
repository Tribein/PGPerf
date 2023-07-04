/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local.pgperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.log.MLevel;
import com.mchange.v2.log.MLogger;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
//import java.util.logging.Handler;
//import java.util.logging.Level;
//import java.util.logging.LogManager;
//import java.util.logging.Logger;
import oracle.jdbc.OracleConnection;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import java.util.TimeZone;


public class PGPerf implements Configurable {
    
    private static SLF4JLogger lg;    
    private static final String PROPERTIESFILENAME = "pgperf.properties";
    private static final int SECONDSTOSLEEP = 60;
    private static Scanner fileScanner;
    private static ArrayList<String> pgDBList;
    private static String DBLISTFILENAME = "db.lst";
    private static String DBUSERNAME = "";
    private static String DBPASSWORD = "";
    private static String CKHUSERNAME = "";
    private static String CKHPASSWORD = "";
    private static String DBLISTSOURCE = "";  
    private static String CKHCONNECTIONSTRING = "";
    private static int CKHQUEUECONSUMERS = 1;
    private static boolean GATHERSESSIONS = false;
    private static boolean GATHERSESSTATS = false;
    private static boolean GATHERSYSSTATS = false;
    private static boolean GATHERSQLSTATS = false;    
    private static String ORADBLISTCSTR = "";
    private static String ORADBLISTUSERNAME = "";
    private static String ORADBLISTPASSWORD = "";
    private static String ORADBLISTQUERY = "";
    static Map<String, Thread> dbSessionsList = new HashMap();
    static Thread[] ckhQueueThreads;
    private static ComboPooledDataSource CKHDataSource;
    private static BlockingQueue<PgCkhMsg> ckhQueue = new LinkedBlockingQueue();    

    private static boolean processProperties(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
            DBUSERNAME = properties.getProperty("DBUSERNAME");
            DBPASSWORD = properties.getProperty("DBPASSWORD");
            CKHUSERNAME = properties.getProperty("CKHUSERNAME");
            CKHPASSWORD = properties.getProperty("CKHPASSWORD");
            CKHCONNECTIONSTRING = properties.getProperty("CKHCONNECTIONSTRING");
            DBLISTSOURCE = properties.getProperty("DBLISTSOURCE");
            CKHQUEUECONSUMERS = Integer.parseInt(properties.getProperty("QUEUECONSUMERS"));
            switch (DBLISTSOURCE.toUpperCase()) {
                case "FILE":
                    DBLISTFILENAME = properties.getProperty("DBLISTFILENAME");
                    break;
                case "ORADB":
                    ORADBLISTCSTR = properties.getProperty("ORADBLISTCONNECTIONSTRING");
                    ORADBLISTUSERNAME = properties.getProperty("ORADBLISTUSERNAME");
                    ORADBLISTPASSWORD = properties.getProperty("ORADBLISTPASSWORD");
                    ORADBLISTQUERY = properties.getProperty("ORADBLISTQUERY");
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    break;
                default:
                    lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                            + "No proper database list source was provided!"
                    );
            }
            if (properties.getProperty("SESSIONS").compareToIgnoreCase("TRUE") == 0) {
                GATHERSESSIONS = true;
            }
            if (properties.getProperty("SESSTATS").compareToIgnoreCase("TRUE") == 0) {
                GATHERSESSTATS = true;
            }
            if (properties.getProperty("SYSSTATS").compareToIgnoreCase("TRUE") == 0) {
                GATHERSYSSTATS = true;
            }
            if (properties.getProperty("SQLSTATS").compareToIgnoreCase("TRUE") == 0) {
                GATHERSQLSTATS = true;
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    private static void processCKHQueueConsumers() {
        for (int i = 0; i < CKHQUEUECONSUMERS; i++) {
            if ((ckhQueueThreads[i] == null) || (!ckhQueueThreads[i].isAlive())) {
                lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                        + "Starting clickhouse queue consumer #" + i
                );

                ckhQueueThreads[i] = new CkhQueueConsumer(ckhQueue, CKHDataSource);
                ckhQueueThreads[i].start();
            }
        }
    }    
    private static ComboPooledDataSource initDataSource() {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.clickhouse.ClickHouseDriver");
            cpds.setJdbcUrl(CKHCONNECTIONSTRING);
            cpds.setUser(CKHUSERNAME);
            cpds.setPassword(CKHPASSWORD);
            cpds.setMinPoolSize(100);
            cpds.setAcquireIncrement(100);
            cpds.setMaxPoolSize(5120);
            cpds.setMaxIdleTime(120);
            cpds.setNumHelperThreads(8);
            cpds.setForceSynchronousCheckins(true);
            //cpds.setTestConnectionOnCheckout(true);
            //cpds.setTestConnectionOnCheckin(true);
            //cpds.setIdleConnectionTestPeriod(30);
            //cpds.setPreferredTestQuery("select 1 from dual");
            return cpds;
        } catch (Exception e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Cannot connect to ClickHouse server!"
            );
        }
        return null;
    }
    private static ArrayList<String> getListFromOraDB(String cstr, String usn, String pwd, String query)
            throws ClassNotFoundException, SQLException {
        ArrayList<String> retList = new ArrayList();
        Properties props = new Properties();
        props.setProperty(OracleConnection.CONNECTION_PROPERTY_USER_NAME, usn);
        props.setProperty(OracleConnection.CONNECTION_PROPERTY_PASSWORD, pwd);
        props.setProperty(OracleConnection.CONNECTION_PROPERTY_NET_KEEPALIVE, "true");
        props.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_NET_CONNECT_TIMEOUT, "10000");
        props.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_READ_TIMEOUT, "180000");
        props.setProperty(OracleConnection.CONNECTION_PROPERTY_AUTOCOMMIT, "false");
        Connection dbListcon = DriverManager.getConnection(cstr, props);
        dbListcon.setAutoCommit(false);
        Statement dbListstmt = dbListcon.createStatement(/*1005, 1007*/);
        ResultSet dbListrs = dbListstmt.executeQuery(query);
        while (dbListrs.next()) {
            retList.add(dbListrs.getString(1));
        }
        dbListrs.close();
        dbListstmt.close();
        dbListcon.close();
        return retList;
    }

    private static ArrayList<String> getListFromFile(File dbListFile) {
        ArrayList<String> retList = new ArrayList();
        try {
            fileScanner = new Scanner(dbListFile);
            while (fileScanner.hasNext()) {
                retList.add(fileScanner.nextLine());
            }
            fileScanner.close();
        } catch (FileNotFoundException e) {
            System.out.println("Error reading database list!");
            e.printStackTrace();
        } finally {
            return retList;
        }
    }
    
    private static ArrayList<String> getPgDBList() {
        try {
            switch (DBLISTSOURCE.toUpperCase()) {
                case "FILE":
                    return getListFromFile(new File(DBLISTFILENAME));
                case "ORADB":
                    return getListFromOraDB(ORADBLISTCSTR, ORADBLISTUSERNAME, ORADBLISTPASSWORD, ORADBLISTQUERY);
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    private static void processSessions(String dbLine) {
        if ((!dbSessionsList.containsKey(dbLine))
                || (!dbSessionsList.get(dbLine).isAlive())) {
            try {
                if (dbSessionsList.containsKey(dbLine)) {
                    dbSessionsList.remove(dbLine);
                }
                dbSessionsList.put(
                    dbLine, 
                    new StatCollector(
                            dbLine, 
                            DBUSERNAME, 
                            DBPASSWORD, 
                            CKHDataSource, 
                            THREADWAITS, 
                            ckhQueue
                    )
                );
                lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbLine
                        + "\t" + "starting sessions waits thread"
                );

                dbSessionsList.get(dbLine).start();
            } catch (Exception e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbLine
                        + "\t" + "error running sessions thread"
                );

                e.printStackTrace();
            }
        } else {
            /*
            lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbLine
                    + "\t" + "sessions thread state"
                    + "\t" + dbSessionsList.get(dbLine).getState().toString()
            );
            */
        }
    }    
    public static void main(String[] args) throws InterruptedException {
/*
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.INFO);
        for (Handler h : rootLogger.getHandlers()) {
            h.setLevel(Level.WARNING);
        }     
*/
        //System.setProperty("user.timezone", "UTC");
        //TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        
        Logger rootLogger = (Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.ERROR);
        
        lg = new SLF4JLogger();
        lg.configure();
        if (!processProperties(PROPERTIESFILENAME)) {
            System.exit(1);
        }            
        ckhQueueThreads = new Thread[CKHQUEUECONSUMERS];

        CKHDataSource = initDataSource();
        
        if (CKHDataSource == null) {
            System.exit(2);
        }
        while (true) {
            
            processCKHQueueConsumers();

            pgDBList = getPgDBList();

            if(pgDBList==null){
                lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                        + "NULL database list got from source!"
                );  
                System.exit(4);
            }
            
            if (pgDBList.isEmpty() ){
                lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                        + "Empty database list got from source!"
                );                
            }
            lg.LogInfo("Databases list size:"+pgDBList.size());
            for (int i = 0; i < pgDBList.size(); i++) {
                String dbLine = pgDBList.get(i);
                if (GATHERSESSIONS) {
                    processSessions(dbLine);
                }
                /*
                if (GATHERSESSTATS) {
                    processSessionStats(dbLine);
                }
                if (GATHERSYSSTATS) {
                    processSystemRoutines(dbLine);
                }
                if (GATHERSQLSTATS) {
                    processSQLRoutines(dbLine);
                }
                */
            }
            TimeUnit.SECONDS.sleep(SECONDSTOSLEEP);
        }        
    }
}
