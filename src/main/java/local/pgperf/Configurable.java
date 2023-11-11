/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local.pgperf;

import java.time.Clock;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

public interface Configurable {
  public static final int RSSESSIONWAIT       = 0;
  public static final int RSSESSIONSTAT       = 1;
  public static final int RSSYSTEMSTAT        = 2;
  public static final int RSSQLSTAT           = 3;
  public static final int RSSEGMENTSTAT       = 4;
  public static final int RSSQLPHV            = 5;
  public static final int RSSQLTEXT           = 6;
  public static final int RSSTATNAME          = 7;
  public static final int RSIOFILESTAT        = 8;
  public static final int RSIOFUNCTIONSTAT    = 9;
  public static final int RSFILESSIZE         = 10;
  public static final int RSSEGMENTSSIZE      = 11;
  
  public static final int THREADWAITS         = 0;
  public static final int THREADSESSION       = 1;
  public static final int THREADSYSTEM        = 2;
  public static final int THREADSQL           = 3;
  
  public static final int SECONDSBETWEENSQLSNAPS            = 60;
  public static final int SECONDSBETWEENSESSSTATSSNAPS      = 30;
  public static final int SECONDSBETWEENSESSWAITSSNAPS      = 5;
  public static final int SECONDSBETWEENSYSSTATSSNAPS       = 10;
  
  public static final Calendar MYTZCAL                        = Calendar.getInstance(TimeZone.getTimeZone("Europe/Moscow"));  
  public static final Clock MYTZCLOACK                               = Clock.system(ZoneId.of("Europe/Moscow"));
  public static final DateTimeFormatter 
                          DATEFORMAT          = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");    
}
