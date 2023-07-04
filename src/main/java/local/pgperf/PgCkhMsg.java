/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local.pgperf;

import java.util.List;

public class PgCkhMsg {
    public int dataType; 
    public long currentDateTime;
    public String dbName;
    public String dbHost;
    public List dataList;
    
   
    public PgCkhMsg( int type, long dt, String db, String host, List lst ){
        currentDateTime   = dt;
        dataList          = lst;
        dbName            = db;
        dbHost            = host;
        dataType          = type;
    }    
}
