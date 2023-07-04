/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local.pgperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import oracle.jdbc.OracleConnection;

public class StatCollector
        extends Thread implements Configurable {

    private final SLF4JLogger lg;
    private final int threadType;
    private final long sleepTime = 86400000;
    private final String dbUserName;
    private final String dbPassword;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private final BlockingQueue<PgCkhMsg> ckhQueue;

    public StatCollector(String inputString, String dbUSN, String dbPWD, ComboPooledDataSource ckhDS, int runTType, BlockingQueue<PgCkhMsg> queue) {
        dbConnectionString      = inputString;
        dbUniqueName            = inputString.split("/")[1];
        dbHostName              = inputString.split(":")[0];
        dbUserName              = dbUSN;
        dbPassword              = dbPWD;
        threadType              = runTType;
        ckhQueue                = queue;
        lg                      = new SLF4JLogger();
    }

    private void cleanup(Connection con) {
        try {
            if ((con != null) && (!con.isClosed())) {
                con.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString 
                    + "\t" + "error during PG connection cleanup"
                    + "\t" + e.getMessage()
            );
        }
    }

    private Connection openConnection() {
        Connection con=null;
        try {
            Properties props = new Properties();
            props.setProperty(OracleConnection.CONNECTION_PROPERTY_USER_NAME, dbUserName);
            props.setProperty(OracleConnection.CONNECTION_PROPERTY_PASSWORD, dbPassword);
            props.setProperty("user", dbUserName);
            props.setProperty("password", dbPassword);
            con = DriverManager.getConnection("jdbc:postgresql://" + dbConnectionString, props);
            con.setNetworkTimeout(Executors.newSingleThreadExecutor(), 10000);
            //con.setAutoCommit(false);
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "cannot initiate connection to target PG database"
                    + "\t" + e.getMessage()
            );
        }finally{
            return con;
        }
    }
       
    @Override
    public void run() {
        Connection con=null;
        boolean shutdown = false;
        
        Thread.currentThread().setName(dbHostName+"@"+dbUniqueName+"@"+String.valueOf(threadType));

        con = openConnection();
        
        if(con==null){
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "could not acquire proper database connection!" 
            ); 
            return;
        }
        if (!shutdown) {
            try {
                switch (threadType) {
                    case THREADWAITS:
                        WaitsCollector waits = new WaitsCollector(
                                con, 
                                ckhQueue, 
                                dbUniqueName, 
                                dbHostName, 
                                dbConnectionString
                        );
                        waits.RunCollection();
                        break;
                    default:
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString 
                                + "\t" + "unknown thread type provided!"
                        );
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cleanup(con);
        }
    }
}