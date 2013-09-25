package org.sunspotworld.demo;

import com.sun.spot.io.j2me.radiogram.*;

import com.sun.spot.peripheral.ota.OTACommandServer;
import com.sun.spot.util.Utils;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.Date;
import javax.microedition.io.*;


/**
 * Challenge 2
 */
public class DatabaseInsertHelper {
    // Broadcast port on which we listen for sensor samples
    private static final int HOST_PORT = 65;
        
    private void run() throws Exception {
        RadiogramConnection rCon;
        Datagram dg;
        DateFormat fmt = DateFormat.getTimeInstance();
         
        try {
            // Open up a server-side broadcast radiogram connection
            // to listen for sensor readings being sent by different SPOTs
            rCon = (RadiogramConnection) Connector.open("radiogram://:" + HOST_PORT);
            dg = rCon.newDatagram(rCon.getMaximumLength());
        } catch (Exception e) {
             System.err.println("setUp caught " + e.getMessage());
             throw e;
        }

        /*Create table*/
        createTable();
        
        // Main data collection loop
        while (true) {
            try {
                // Read sensor sample received over the radio
                rCon.receive(dg);
                String addr = dg.getAddress();  // read sender's Id
                long time = dg.readLong();      // read time of the reading
                float temp = dg.readFloat();         // read the sensor value
                
                /*insert into table*/
                insertTable(addr,temp,time);
                System.out.println(fmt.format(new Date(time)) + "  from: " + addr + "   value = " + temp);
            } catch (Exception e) {
                System.err.println("Caught " + e +  " while reading sensor samples.");
                throw e;
            }
        }
    }
    
    /**
     * Start up the host application.
     *
     * @param args any command line arguments
     */
    public static void main(String[] args) throws Exception {
        // register the application's name with the OTA Command server & start OTA running
        OTACommandServer.start("SendDataDemo");

        DatabaseInsertHelper app = new DatabaseInsertHelper();
        app.run();
    }
    
    /* Method to create table with three rows*/
    /*Create table code based on code at: http://www.tutorialspoint.com/sqlite/sqlite_java.htm*/
    public static void createTable() {
        java.sql.Connection createConnect = null;
        Statement sqlStatement = null;
        
        try {
            
            /*Create connection with database*/
            Class.forName("org.sqlite.JDBC");
            createConnect = DriverManager.getConnection("jdbc:sqlite:spotData.db");
            System.out.println("Opened database successfully");

            /*Create table sql statement*/
            sqlStatement = createConnect.createStatement();
            String sql = "CREATE TABLE OURDATA"
                    + "(ID             VARCHAR   NOT NULL,"
                    + " TEMP           VARCHAR    NOT NULL, "
                    + " TIME           LONG     NOT NULL,"
                    + "CONSTRAINT pri_id_time PRIMARY KEY(ID,TIME))";
            
            /*Execute sql statement and close connection*/
            try{
            sqlStatement.executeUpdate(sql);
            }catch(Exception e){
                System.err.println("Table already exists...continuing");
            }
            sqlStatement.close();
            createConnect.close();
            
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Table created successfully");
    }
    
    /*method to insert table*/
    public static void insertTable(String id, float temp, long time)
    {
        java.sql.Connection insertConnection = null;
        Statement insertStatement = null;
        
        try 
        {
            /*Create connection with database*/
            Class.forName("org.sqlite.JDBC");
            insertConnection = DriverManager.getConnection("jdbc:sqlite:spotData.db");
            System.out.println("Opened database successfully");
            
            /*Create sql statement*/
            insertStatement = insertConnection.createStatement();
            String sql = "INSERT INTO OURDATA(ID,TEMP,TIME)" + 
                    "VALUES('"+id+"','"+temp+"',"+time+");";
            
            /*Execute sql statement and close connection*/
            insertStatement.executeUpdate(sql);
            insertStatement.close();
            insertConnection.close();
            
        }catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }
}
