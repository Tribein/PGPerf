/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local.pgperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
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
  
 @Override
  public void run()
  {
    lg = new SLF4JLogger();
    try{
      Connection ckhConnection = ckhDataSource.getConnection();
      PreparedStatement ckhPreparedStatement = null;
      switch (dataType)
      {
      case RSSESSIONWAIT: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSESSIONSQUERY);
        processSessions(ckhPreparedStatement, dataList, dataTS);
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
