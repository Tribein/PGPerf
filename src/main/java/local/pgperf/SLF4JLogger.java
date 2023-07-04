/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local.pgperf;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import org.slf4j.LoggerFactory;

public class SLF4JLogger {

    private final Logger slf4jLogger = (Logger) LoggerFactory.getLogger(SLF4JLogger.class);
    SLF4JLogger(){

    }
    public void configure() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        PatternLayoutEncoder ple = new PatternLayoutEncoder();
        ple.setPattern("%date %level %msg%n");
        ple.setContext(lc);
        ple.start();
        ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
        consoleAppender.setEncoder(ple);
        consoleAppender.setContext(lc);
        consoleAppender.start();
        slf4jLogger.addAppender(consoleAppender);
        slf4jLogger.setLevel(Level.INFO);
        slf4jLogger.setAdditive(false);
    }
    public void LogWarn(String message) {

        slf4jLogger.warn(message);
    }
    
    public void LogInfo(String message) {

        slf4jLogger.info(message);
    }
    
    public void LogTrace(String message){
        
        slf4jLogger.trace(message);
        
    }
    public void LogError(String message) {

        slf4jLogger.error(message);
    }        
    
}
