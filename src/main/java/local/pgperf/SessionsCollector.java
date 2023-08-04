/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local.pgperf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SessionsCollector implements Configurable {

    private final SLF4JLogger lg;
    private final Connection con;
    private final BlockingQueue<PgCkhMsg> ckhQueue;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private static final String PGSESSWAITSQUERY12 = 
        "select " +
            "coalesce(datname,'n/a') as datname, " +
            "pid, " +
            "0/*coalesce(leader_pid,0)*/ as leader_pid, " +
            "coalesce((pg_blocking_pids(pid))[1],0) as blocked_by, " +
            "coalesce(trim(application_name),'n/a') as application_name, " +
            "coalesce(cast(client_addr as text),'n/a') as client_addr, " +
            "coalesce(client_port,0) as client_port, " +
            "backend_start, " +
            "coalesce(xact_start,('2000-01-01 00:00:00+00'::timestamptz)) as xact_start, " +
            "coalesce(query_start,('2000-01-01 00:00:00+00'::timestamptz)) as query_start, " +
            "coalesce(state_change,('2000-01-01 00:00:00+00'::timestamptz)) as state_change, " +
            "coalesce(wait_event_type,'n/a') as wait_event_type, " +
            "coalesce(wait_event,'n/a') as wait_event, " +
            "coalesce(state,'n/a') as state, " +
            "0/*coalesce(query_id,0)*/ as query_id, " +
            "backend_type, " +
            "coalesce(cast(cast(backend_xid as text) as bigint),0) as backend_xid, " +
            "coalesce(cast(cast(backend_xmin as text) as bigint),0) as backend_xmin " +
        "from pg_catalog.pg_stat_activity "+
        "where (" + 
            "(coalesce(state,'-')<>'idle' and coalesce(wait_event,'-')<>'ClientRead') "+
            "or state_change  > current_timestamp  - interval '"+SECONDSBETWEENSESSWAITSSNAPS+"' second"+
        ") and pid<>pg_backend_pid()"
        ;
    private static final String PGSESSWAITSQUERY15 = 
        "select " +
            "coalesce(datname,'n/a') as datname, " +
            "pid, " +
            "0/*coalesce(leader_pid,0)*/ as leader_pid, " +
            "coalesce((pg_blocking_pids(pid))[1],0) as blocked_by, " +
            "coalesce(trim(application_name),'n/a') as application_name, " +
            "coalesce(cast(client_addr as text),'n/a') as client_addr, " +
            "coalesce(client_port,0) as client_port, " +
            "backend_start, " +
            "coalesce(xact_start,('2000-01-01 00:00:00+00'::timestamptz)) as xact_start, " +
            "coalesce(query_start,('2000-01-01 00:00:00+00'::timestamptz)) as query_start, " +
            "coalesce(state_change,('2000-01-01 00:00:00+00'::timestamptz)) as state_change, " +
            "coalesce(wait_event_type,'n/a') as wait_event_type, " +
            "coalesce(wait_event,'n/a') as wait_event, " +
            "coalesce(state,'n/a') as state, " +
            "coalesce(query_id,0) as query_id, " +
            "backend_type, " +
            "coalesce(cast(cast(backend_xid as text) as bigint),0) as backend_xid, " +
            "coalesce(cast(cast(backend_xmin as text) as bigint),0) as backend_xmin " +
        "from pg_catalog.pg_stat_activity "+
        "where (" + 
            "(coalesce(state,'-')<>'idle' and coalesce(wait_event,'-')<>'ClientRead') "+
            "or state_change  > current_timestamp  - interval '"+SECONDSBETWEENSESSWAITSSNAPS+"' second"+
        ") and pid<>pg_backend_pid()"
        ;    
    private static final String PGVERSIONQUERY = "select regexp_substr(version(),'\\d+',1,1)::int";
    public SessionsCollector(Connection connection, BlockingQueue<PgCkhMsg> queue, String dbname, String dbhost, String connstr) {
        ckhQueue                = queue;
        con                     = connection;
        dbConnectionString      = connstr;
        dbUniqueName            = dbname;
        dbHostName              = dbhost;
        lg                      = new SLF4JLogger();
    }

    private List getSessionWaitsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getInt(2));
                rowList.add(rs.getInt(3));
                rowList.add(rs.getInt(4));
                rowList.add(rs.getString(5));
                rowList.add(rs.getString(6));
                rowList.add(rs.getInt(7));
                //
                rowList.add(rs.getTimestamp(8,MYTZCAL).getTime() / 1000L);
                rowList.add(rs.getTimestamp(9,MYTZCAL).getTime() / 1000L);
                rowList.add(rs.getTimestamp(10,MYTZCAL).getTime() / 1000L);
                rowList.add(rs.getTimestamp(11,MYTZCAL).getTime() / 1000L);
                //
                rowList.add(rs.getString(12));
                rowList.add(rs.getString(13));
                rowList.add(rs.getString(14));
                rowList.add(rs.getLong(15));
                rowList.add(rs.getString(16));
                rowList.add(rs.getLong(17));
                rowList.add(rs.getLong(18));
                //
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from waits resultset"
                    + "\t" + e.getMessage()
            );
            e.printStackTrace();
        }
        return outList;
    }

    private void cleanup(PreparedStatement oraWaitsPreparedStatement) {
        try {
            if(this.con.isClosed()){
                oraWaitsPreparedStatement=null;
                return;
            }
            if ((oraWaitsPreparedStatement != null) && (!oraWaitsPreparedStatement.isClosed())) {
                oraWaitsPreparedStatement.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error durring PG resource cleanups"
                    + "\t" + e.getMessage()
            );
        }
    }
    private int getVersion (Connection con) {
            int version = 0;
            ResultSet rs = null;
            Statement stmt = null;
            try{
                stmt = con.createStatement();
                rs = stmt.executeQuery(PGVERSIONQUERY);
                if(rs.next()){
                    version = rs.getInt(1);
                }
                rs.close();
                stmt.close();
                return version;
            }catch(Exception e){
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "cannot get version from database" 
                    + "\t" + e.getMessage()
                );                
                if( rs != null || !rs.isClosed()){
                    rs.close();
                }
                if ( stmt != null || ! stmt.isClosed()){
                    stmt.close();
                }
                version = 0;
            }finally{
                return version;
            }
    }
    public void RunCollection() throws InterruptedException {
        PreparedStatement pgWaitsPreparedStatement=null;
        boolean shutdown = false;    
        
        int version = getVersion(con);

        try {
            pgWaitsPreparedStatement = 
                    con.prepareStatement(
                        (
                                (version>=14)? PGSESSWAITSQUERY15 : PGSESSWAITSQUERY12
                        )
                    );
            pgWaitsPreparedStatement.setFetchSize(1000);
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "cannot prepare statements"
            );
            shutdown = true;
        }
        while (!shutdown) {
            try {
                pgWaitsPreparedStatement.execute();
                ckhQueue.put(
                        new PgCkhMsg(
                                RSSESSIONWAIT, 
                                Instant.now().getEpochSecond(), 
                                dbUniqueName, 
                                dbHostName,
                                getSessionWaitsListFromRS(pgWaitsPreparedStatement.getResultSet())
                        )
                );
                pgWaitsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error getting sessions from database"
                        + "\t" + e.getMessage()
                );
                shutdown = true;
            }
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSESSWAITSSNAPS);
        }
        cleanup(pgWaitsPreparedStatement);
    }
}