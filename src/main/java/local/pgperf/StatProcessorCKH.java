
package local.pgperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class StatProcessorCKH extends Thread implements Configurable{
    
  SLF4JLogger lg;
  private ComboPooledDataSource ckhDataSource;
  private final int dataType;
  private final long dataTS;
  private final List dataList;
  private final String dbName;
  private final String dbHost;    
  private final String CKHINSERTSESSIONSQUERY = "insert into sessions_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  private final String CKHINSERTSQLTEXTSQUERY = "insert into sqltexts_buffer values (?,?)";
  private final String CKHINSERTSQLSTATSQUERY = "insert into sqlstats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  
  public StatProcessorCKH(int inpType, long inpTS, String inpDBName, String inpDBHost, ComboPooledDataSource ckhDS, List inpList)
  {
    dbName        = inpDBName;
    dbHost          = inpDBHost;
    dataList            = inpList;
    dataType            = inpType;
    ckhDataSource       = ckhDS;
    dataTS              = inpTS;
  }  
  
  public void processSessions(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setLong(1, currentDateTime);
        prep.setString(2, dbHost);
        prep.setString(3, (String)row.get(0));
        prep.setInt(4,((int)row.get(1)));
        prep.setInt(5,((int)row.get(2)));
        prep.setInt(6,((int)row.get(3)));
        prep.setString(7, (String)row.get(4));
        prep.setString(8, (String)row.get(5));
        prep.setInt(9,((int)row.get(6)));
        //
        prep.setLong(10, ((long)row.get(7)));
        prep.setLong(11, ((long)row.get(8)));
        prep.setLong(12, ((long)row.get(9)));
        prep.setLong(13, ((long)row.get(10)));
        //
        prep.setString(14, (String)row.get(11));
        prep.setString(15, (String)row.get(12));
        prep.setString(16, (String)row.get(13));
        prep.setLong(17, ((long)row.get(14)));
        prep.setString(18, (String)row.get(15));
        prep.setLong(19, ((long)row.get(16)));
        prep.setLong(20, ((long)row.get(17)));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + 
              "\t" + dbName + "\t" + dbHost + "\tError submitting sessions data to ClickHouse!"
      );
      
      e.printStackTrace();
    }
  }
  public void processSQLTexts(PreparedStatement prep, List lst)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setLong(1, (long)row.get(1));
        prep.setString(2, (String)row.get(0));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbHost + "\t"+"Error processing sql texts!"
      );
      
      e.printStackTrace();
    }
  }  
  public void processSQLStats(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setLong(1, currentDateTime);
        prep.setString(2, dbHost);
        prep.setString(3, (String) row.get(2));
        prep.setLong(4, (Long) row.get(1) );
        prep.setString(5, (String) row.get(3));
        prep.setInt(6, ( (boolean) row.get(4))? 1 : 0 );
        prep.setLong(7, (Long) row.get(5) );
        prep.setLong(8, (Long) row.get(6) );
        prep.setLong(9, (Long) row.get(7) );
        //--plan time
        prep.setDouble(10, (Double) row.get(8) );
        prep.setDouble(11, (Double) row.get(9) );
        prep.setDouble(12, (Double) row.get(10) );
        //--exec time
        prep.setDouble(13, (Double) row.get(11) );
        prep.setDouble(14, (Double) row.get(12) );
        prep.setDouble(15, (Double) row.get(13) );        
        //--shared block counters
        prep.setLong(16, (Long) row.get(14) );
        prep.setLong(17, (Long) row.get(15) );
        prep.setLong(18, (Long) row.get(16) );        
        //--local block counters 
        prep.setLong(19, (Long) row.get(17) );
        prep.setLong(20, (Long) row.get(18) );
        prep.setLong(21, (Long) row.get(19) );                
        prep.setLong(22, (Long) row.get(20) );   
        //--temp block counters
        prep.setLong(23, (Long) row.get(21) );                
        prep.setLong(24, (Long) row.get(22) );           
        //--block timings
        prep.setDouble(25, (Double) row.get(23) );
        prep.setDouble(26, (Double) row.get(24) );
        prep.setDouble(27, (Double) row.get(25) );           
        prep.setDouble(28, (Double) row.get(26) );   
        //--wal
        prep.setLong(29, (Long) row.get(27) );
        prep.setLong(30, (Long) row.get(28) );
        prep.setLong(31, (Long) row.get(29) );
        //--jit counters
        prep.setLong(32, (Long) row.get(30) );
        prep.setLong(33, (Long) row.get(31) );
        prep.setLong(34, (Long) row.get(31) );        
        prep.setLong(35, (Long) row.get(33) );
        //--jit time
        prep.setDouble(36, (Double) row.get(34) );
        prep.setDouble(37, (Double) row.get(35) );
        prep.setDouble(38, (Double) row.get(36) );           
        prep.setDouble(39, (Double) row.get(37) ); 
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbHost + "\t"+"Error processing sql stats!"
      );
      
      e.printStackTrace();
    }
  }  
  
  @Override
  public void run()
  {
    lg = new SLF4JLogger();
    try{
      Connection ckhConnection = ckhDataSource.getConnection();
      PreparedStatement ckhPreparedStatement = null;
      switch (dataType){
        case RSSESSIONWAIT: 
            ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSESSIONSQUERY);
            processSessions(ckhPreparedStatement, dataList, dataTS);
        break;
        case RSSQLTEXT: 
            ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSQLTEXTSQUERY);
            processSQLTexts(ckhPreparedStatement, dataList);
        break;   
        case RSSQLSTAT: 
            ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSQLSTATSQUERY);
            processSQLStats(ckhPreparedStatement, dataList,dataTS);
        break;        
        default: 
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Unsupported run type: " + dataType
            );
      }
      if ((ckhPreparedStatement != null) && (!ckhPreparedStatement.isClosed())) {
        ckhPreparedStatement.close();
      }
      if ((ckhConnection != null) && (!ckhConnection.isClosed())) {
        ckhConnection.close();
      }
      ckhDataSource = null;
    } catch (SQLException e){
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
            "Cannot connect to ClickHouse server!"
      );
      
      e.printStackTrace();
    }
  }  
}
