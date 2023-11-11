package local.pgperf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SQLsCollector implements Configurable {
    private final SLF4JLogger lg;
    private final Connection con;
    private final BlockingQueue<PgCkhMsg> ckhQueue;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private static final String PGSQLSTATSQUERY = 
            "select " +
            "	substr(query,1,1024)::varchar as query,queryid,datname,usename," +
            "	toplevel,plans,calls,rows," +
            "	total_plan_time,min_plan_time,max_plan_time," +
            "	total_exec_time,min_exec_time,max_exec_time," +
            "	shared_blks_hit,shared_blks_read,shared_blks_written," +
            "	local_blks_hit,local_blks_read,local_blks_dirtied,local_blks_written," +
            "	temp_blks_read,temp_blks_written," +
            "	blk_read_time,blk_write_time,temp_blk_read_time,temp_blk_write_time," +
            "	wal_records,wal_fpi,wal_bytes," +
            "	jit_functions,jit_inlining_count,jit_optimization_count,jit_emission_count," +
            "	jit_generation_time,jit_inlining_time,jit_optimization_time,jit_emission_time " +
            "from pg_stat_statements as pgss " +
            "join pg_user as pgu on (pgu.usesysid=pgss.userid) " +
            "join pg_database as pgd on (pgd.oid=pgss.dbid)";
    private static final String PGSQLRESETQUERY = "select pg_stat_statements_reset()";

    public SQLsCollector(Connection conn, BlockingQueue<PgCkhMsg> queue, String dbname, String dbhost, String connstr) {
        ckhQueue                = queue;
        con                     = conn;
        dbConnectionString      = connstr;
        dbUniqueName            = dbname;
        dbHostName              = dbhost;
        lg                      = new SLF4JLogger();
    }

    private void cleanup(PreparedStatement pgSQLsPreparedStatement, PreparedStatement pgSQLsResetPreparedStatement) {
        try {
            if(this.con.isClosed()){
                pgSQLsPreparedStatement = null;
                pgSQLsResetPreparedStatement = null;
                return;
            }
            if ((pgSQLsPreparedStatement != null) && (!pgSQLsPreparedStatement.isClosed())) {
                pgSQLsPreparedStatement.close();
            }
            if ((pgSQLsResetPreparedStatement != null) && (!pgSQLsResetPreparedStatement.isClosed())) {
                pgSQLsResetPreparedStatement.close();
            }            
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString + 
                    "\t" + "error during PG resource cleanups" + 
                    "\t" + e.getMessage()
            );

            //e.printStackTrace();
        }
    }

    private List [] getSQlsListFromRS(ResultSet rs) {
        List<List> [] outList = new List[2];
        List<List> statsList = new ArrayList();
        List<List> textsList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List textRowList = new ArrayList();
                List statsRowList = new ArrayList();
                //--
                textRowList.add(rs.getString(1));
                textRowList.add(rs.getLong(2));
                textsList.add(textRowList);
                //--
                statsRowList.add(rs.getString(1));
                statsRowList.add(rs.getLong(2));
                statsRowList.add(rs.getString(3));
                statsRowList.add(rs.getString(4));
                //
                statsRowList.add(rs.getBoolean(5));
                statsRowList.add(rs.getLong(6));
                statsRowList.add(rs.getLong(7));
                statsRowList.add(rs.getLong(8));
                //
                statsRowList.add(rs.getDouble(9));
                statsRowList.add(rs.getDouble(10));
                statsRowList.add(rs.getDouble(11));
                statsRowList.add(rs.getDouble(12));
                statsRowList.add(rs.getDouble(13));
                statsRowList.add(rs.getDouble(14));
                //
                statsRowList.add(rs.getLong(15));
                statsRowList.add(rs.getLong(16));
                statsRowList.add(rs.getLong(17));
                statsRowList.add(rs.getLong(18));
                statsRowList.add(rs.getLong(19));
                statsRowList.add(rs.getLong(20));
                statsRowList.add(rs.getLong(21));
                statsRowList.add(rs.getLong(22));
                statsRowList.add(rs.getLong(23));
                //
                statsRowList.add(rs.getDouble(24));
                statsRowList.add(rs.getDouble(25));
                statsRowList.add(rs.getDouble(26));
                statsRowList.add(rs.getDouble(27));                
                //
                statsRowList.add(rs.getLong(28));
                statsRowList.add(rs.getLong(29));
                statsRowList.add(rs.getLong(30));                
                //
                statsRowList.add(rs.getLong(31));
                statsRowList.add(rs.getLong(32));
                statsRowList.add(rs.getLong(33));                    
                statsRowList.add(rs.getLong(34));    
                //
                statsRowList.add(rs.getDouble(35));
                statsRowList.add(rs.getDouble(36));
                statsRowList.add(rs.getDouble(37));
                statsRowList.add(rs.getDouble(38));                
                //
                statsList.add(statsRowList);
                //--
            }
            rs.close();
            outList[0] = statsList;
            outList[1] = textsList;
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from sql stats resultset"
                    + "\t" + e.getMessage()
            );
            //outList.clear();
            e.printStackTrace();
        }
        return outList;
    }    
    
    private boolean collectSQLStats(PreparedStatement stmtData/*, PreparedStatement stmtReset*/) throws InterruptedException {
            List[] sqlsRSs = new List [2];
            long ts = Instant.now().getEpochSecond();
            try {
                stmtData.execute();
                sqlsRSs = getSQlsListFromRS(stmtData.getResultSet());
                
                ckhQueue.put(
                        new PgCkhMsg(
                                RSSQLSTAT, 
                                ts , 
                                dbUniqueName, 
                                dbHostName,
                                sqlsRSs[0]
                        )
                );
                ckhQueue.put(
                        new PgCkhMsg(
                                RSSQLTEXT, 
                                ts, 
                                dbUniqueName, 
                                dbHostName,
                                sqlsRSs[1]
                        )
                );                
                stmtData.clearWarnings();
                /*
                stmtReset.execute();
                stmtReset.clearWarnings();
                */
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error processing sql stats!"
                        + "\t" + e.getMessage()
                );
                //e.printStackTrace();
                return true;
            }        
        return false;
    }
        
    public void RunCollection() throws InterruptedException {
        PreparedStatement pgSQLsPreparedStatement=null;
        PreparedStatement pgSQLsResetPreparedStatement=null;
        boolean shutdown = false;
        long begints,endts;
        try {
            pgSQLsPreparedStatement = con.prepareStatement(PGSQLSTATSQUERY);
            pgSQLsPreparedStatement.setFetchSize(10000);
            pgSQLsResetPreparedStatement = con.prepareStatement(PGSQLRESETQUERY);
            pgSQLsPreparedStatement.setFetchSize(1);
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Cannot prepare statements for PG database: " + dbConnectionString
            );
            shutdown = true;
        }
        while (!shutdown) {
            begints = System.currentTimeMillis();
            shutdown = collectSQLStats(pgSQLsPreparedStatement/*,pgSQLsResetPreparedStatement*/);
            endts = System.currentTimeMillis();
            if (endts - begints < SECONDSBETWEENSQLSNAPS * 1000L) {
                TimeUnit.SECONDS.sleep(
                        SECONDSBETWEENSQLSNAPS - (int) ( (endts - begints) / 1000L )
                );
            }                            
        }
        cleanup(pgSQLsPreparedStatement,pgSQLsResetPreparedStatement);
    }    
}
