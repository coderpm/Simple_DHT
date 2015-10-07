package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    /**
     * Global variables
     */
    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static String myPort;       //Stores my port i.e. 11*** form
    static String useMyport;    //Stores the using port for hashing purposes i.e. 5554 kind

    static ContentValues dhtContentValues;
    static Uri dhtUri;
    static ContentResolver dhtContentResolver;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";


    //Variables for the ring
    static boolean joinStatus=false;          //Holds the joinStatus for the particular node
    static String delimiter ="~!";
    static String messagedelimiter ="#";
    static String previousNodehash;
    static String nextNodehash;
    static int previousNode;
    static int nextNode;
    static String selfHash;
    static HashMap<String,String> ringMap = new HashMap<String, String>();
    static HashMap<String,String> myMap = new HashMap<String,String>();
    static ArrayList<String> keyList = new ArrayList<String>();

    //Variable to handle single query
    static boolean waitSingleQuery=false;
    static HashMap<String,String> originalPortMap = new HashMap<String,String>();
    static HashMap<String,String> singleResultsMap = new HashMap<String,String>();


    //Variable to handle all query
    static boolean waitAllQuery=false;
    static HashMap<String,String> AllResultsMap = new HashMap<String,String>();

    //Variable to handle delete all
    static boolean waitAllDelete=false;

    //Variable to handle delete single
    static boolean waitSingleDelete=false;


    static String result;

    /**
     * Database specific constant declarations
     */

    public static SQLiteDatabase accessdb,readingDb,writingDb;
    static final String DB_NAME = "DHT";
    static final String TABLE_NAME = "dhtMessage";
    static final String firstColumn = "key";
    static final String secondColumn = "value";
    static final int DATABASE_VERSION = 1;

    static final String CREATE_DB_TABLE =
            " CREATE TABLE " + TABLE_NAME +
                    " (key TEXT PRIMARY KEY, " + " value TEXT NOT NULL);";

    private static class accessDBHelp extends SQLiteOpenHelper {
        accessDBHelp(Context context)
        {
            super(context, DB_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db)
        {
            System.out.println("USER:::Creating table query "+CREATE_DB_TABLE);
            try {
                db.execSQL("DROP TABLE IF EXISTS " +  TABLE_NAME);
                db.execSQL(CREATE_DB_TABLE);

            }catch (Exception e)
            {
                e.printStackTrace();
                Log.e(TAG, "Can't create table");
            }
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion,
                              int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " +  TABLE_NAME);
            onCreate(db);
        }
    }

    /**
     * End of Database Specific functions
     *
     */

    /**
     * Start of URI Builder
     */

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    //URI FUNCTIONS

    //Function called first
    @Override
    public boolean onCreate()
    {
        //Initializing the DB variables
        Context con = getContext();
        accessDBHelp dbHelp = new accessDBHelp(getContext());

        SQLiteDatabase cdb = new accessDBHelp(getContext()).getWritableDatabase();
        writingDb = dbHelp.getWritableDatabase();
        readingDb= dbHelp.getReadableDatabase();

        //Tasks for implementing DHT

        //Get own port
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        useMyport = String.valueOf((Integer.parseInt(portStr)));

        try {
            selfHash = genHash(useMyport);
        }catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }

        dhtUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider");
        dhtContentValues = new ContentValues();
        previousNodehash=null;
        nextNodehash=null;

        //Make a server Socket
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            System.out.println("USER::Server socket created for "+myPort);

            //Make the joinStatus as true as the ring has been started

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (Exception e) {
            e.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
            return true;
        }

        //Different startup operations if myPort is 5554 or anything else
        //Convert the port to int
        if(myPort.equals("11108"))
        {
            //I am the ring coordinator and I am supposed to set my predecessor and successor

            try{
                String myHash =genHash(useMyport);
                keyList.add(myHash);        //Adding to KeyList which will hold the sorted array
                ringMap.put(myHash, useMyport);         //Contains mapping key-value as hashValue-port(55** style)
                String prev="nothing";
                String next="nothing";
                String prevNextStr= prev+"!"+next;
                myMap.put(myHash,prevNextStr);          //Contains the mapping as hashvalue- previousnodehash delimiter nextNodeHash
                previousNodehash=null;
                nextNodehash=null;

            }catch(NoSuchAlgorithmException e)
            {
                e.printStackTrace();
                Log.e(TAG, "Can't create the hash");
            }
        }
        else
        {
            //I am not the ring coordinator and I need to send request to 5554 join the ring
            String messageSe="nothing";
            String requestMessage = "join"+delimiter+useMyport+delimiter+messageSe;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestMessage, myPort);

        }

        //Returning default value
        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs)
    {
        long rows;
        System.out.println("USER:: Inside the delete operation with parameters as "+selection);
        if(joinStatus==true)
        {

            if((selection.equals("\"*\"")) || selection.contains("allDelete"))
            {
                try
                {
                    String originalPort=null;

                    System.out.println("Delete all from DHT");


                    if(selection.contains("allDelete"))
                    {
                        //Means it is the forwarded query

                        String [] tempDel = selection.split(delimiter);
                        originalPort=tempDel[1];

                        //Delete mine and forward
                        writingDb.delete(TABLE_NAME,null,null);
                        String delMess= "allDelete"+delimiter+originalPort;
                        Log.v("allDelete", delMess);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delMess, myPort);


                    }
                    else
                    {
                        originalPort=myPort;
                        //I am the originator of the ring all delete
                        String delMess= "allDelete"+delimiter+originalPort;
                        writingDb.delete(TABLE_NAME,null,null);
                        Log.v("allDelete", delMess);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delMess, myPort);
                        waitAllDelete=true;

                        while (true)
                        {
                            //keep looping
                            if(!(waitAllDelete))
                                break;
                        }

                        System.out.println("out of wait channel");

                    }

                }catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            else if((selection.equals("\"@\"")))
            {
                String deleteQuery = "SELECT * FROM " + TABLE_NAME +";";
                System.out.println("USER:: The delete query is " + deleteQuery);
                writingDb.delete(TABLE_NAME,null,null);

                System.out.println("Delete all from your local AVD");

            }
            else
            {
                //Delete for the key specified in selection

                try
                {
                    String keyOriginalHash = genHash(selection);
                    int c1 = selfHash.compareTo(keyOriginalHash);
                    int c2=  selfHash.compareTo(previousNodehash);
                    int c3 = previousNodehash.compareTo(keyOriginalHash);
                    String selections=null;
                    String recvOriginalPort=null;
                    boolean checkMine=false;


                        if(selection.contains(delimiter))
                        {
                            String [] splitQuery= selection.split(delimiter);
                            selections = splitQuery[0].trim();
                            recvOriginalPort = splitQuery[1];

                            keyOriginalHash = genHash(selections);
                            c1 = selfHash.compareTo(keyOriginalHash);
                            c2=  selfHash.compareTo(previousNodehash);
                            c3 = previousNodehash.compareTo(keyOriginalHash);
                            checkMine=false;
                        }
                        else
                        {
                            keyOriginalHash  = genHash(selection);
                            selections=selection;
                            recvOriginalPort=myPort;
                            checkMine=true;

                            c1 = selfHash.compareTo(keyOriginalHash);
                            c2=  selfHash.compareTo(previousNodehash);
                            c3 = previousNodehash.compareTo(keyOriginalHash);
                        }

                    if(c1<0 && c3<0 && c2<0)
                    {
                        System.out.println("Delete Condition 1");
                        //Had Store the key-value pair
//                        writingDb.delete(TABLE_NAME,firstColumn+"="+selections,null);
                        if(checkMine)
                        {
                            //I am the originator of query
                            String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selections+"';";
                            System.out.println("The delete query is "+delQuery);
                            writingDb.execSQL(delQuery);

                        }
                        else
                        {
                            //I am not the originator -  delete and send back success
                            String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selections+"';";
                            System.out.println("The delete query is "+delQuery);
                            writingDb.execSQL(delQuery);
                            String mSend = "deleteSingleSuccess"+delimiter+recvOriginalPort;
                            Log.v("Single Delete Success", "done");
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                        }
                    }
                    else if(c1>0 && c3>0 && c2<0)
                    {
                        //Had Store the key-value pair
                        System.out.println("Delete Condition 2");
//                        writingDb.delete(TABLE_NAME,firstColumn+"="+selections,null);
                        if(checkMine)
                        {
                            //I am the originator of delete op
                            String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selections+"';";
                            System.out.println("The delete query is "+delQuery);
                            writingDb.execSQL(delQuery);

                        }
                        else
                        {
                            //I am not the originator- return success to original
                            String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selections+"';";
                            System.out.println("The delete query is "+delQuery);
                            writingDb.execSQL(delQuery);
                            String mSend = "deleteSingleSuccess"+delimiter+recvOriginalPort;
                            Log.v("Single Delete Success", "done");
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                        }
                    }
                    else if(c1>0 && c3<0 )
                    {
                        System.out.println("Delete Condition 4");
                        //Had Store the key-value pair
//                        writingDb.delete(TABLE_NAME,firstColumn+"="+selections,null);
                        if(checkMine)
                        {
                            //Originator
                            String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selections+"';";
                            System.out.println("The delete query is " + delQuery);
                            writingDb.execSQL(delQuery);
                        }
                        else
                        {
                            //Not the originator- return success
                            String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selections+"';";
                            System.out.println("The delete query is " + delQuery);
                            writingDb.execSQL(delQuery);
                            String mSend = "deleteSingleSuccess"+delimiter+recvOriginalPort;
                            Log.v("Single Delete Success", "done");
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                        }

                    }
                    else if(c1<0)
                    {
                        System.out.println("Delete Condition 3");
                        //Had forwarded the key-value pair-- Forward to nextNode
                        if(checkMine)
                        {
                            System.out.println("I am the originator of the delete operation on "+selections);
                            String mSend = "deleteSingle"+delimiter+recvOriginalPort+delimiter+selections;
                            Log.v("Single Delete", mSend);

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                            waitSingleDelete=true;
                            while (true)
                            {
                                //keep looping
                                if(!(waitSingleDelete))
                                    break;
                            }
                        }
                        else
                        {
                            //the delete is just being forwarded from before
                            String mSend = "deleteSingle"+delimiter+recvOriginalPort+delimiter+selections;
                            Log.v("Single Delete", mSend);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                        }
                    }
                    else
                    {
                        System.out.println("Delete Condition 5");
                        //Had forwarded the key-value pair-- Forward to nextNode
                        if(checkMine)
                        {
                            System.out.println("I am the originator of the delete operation on "+selections);
                            String mSend = "deleteSingle"+delimiter+recvOriginalPort+delimiter+selections;
                            Log.v("Single Delete", mSend);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                            waitSingleDelete=true;
                            while (true)
                            {
                                //keep looping
                                if(!(waitSingleDelete))
                                    break;
                            }

                            return 1;

                        }
                        else
                        {
                            //the delete is just being forwarded from before
                            String mSend = "deleteSingle"+delimiter+recvOriginalPort+delimiter+selections;
                            Log.v("Single Delete", mSend);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                        }
                    }


                }catch(Exception e)
                {
                    e.printStackTrace();
                }


            }
        }
        if(joinStatus==false)
        {
            System.out.println("Inside delte joinStatus false");
            if((selection.equals("\"*\"")) ||(selection.equals("\"@\"")) )
            {
                try
                {
                    String deleteQuery = "SELECT * FROM " + TABLE_NAME +";";
                    System.out.println("USER:: The delete query is " + deleteQuery);
                    writingDb.delete(TABLE_NAME,null,null);

                }catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            else
            {
                System.out.println("Deleting one");
                String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selection+"';";
                System.out.println("The delete query is "+delQuery);
                writingDb.execSQL(delQuery);
                Log.v("delete done", selection);

            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values)
    {
        System.out.println("This is inside insert function");
        long rows=0;

        String []keyVal = new String[1];
        keyVal[0] = (String) values.get("key");
        String keyOriginal=keyVal[0];
        String valueOriginal=(String) values.get("value");
        System.out.println("User:: Insert keys " +keyOriginal+" value is "+valueOriginal);
        if(joinStatus==true)
        {
            //Join Status is true i.e. we are part of the ring
            //get the hash of received String
            try {
                String keyOriginalHash = genHash(keyOriginal);
                int c1 = selfHash.compareTo(keyOriginalHash);
                int c2=  selfHash.compareTo(previousNodehash);
                int c3 = previousNodehash.compareTo(keyOriginalHash);

                if(c1<0 && c3<0 && c2<0)
                {
                    System.out.println(" Insert Condition 1");
                    //Store the key-value pair
                    rows = writingDb.insert(TABLE_NAME, "", values);
                    System.out.println("USER:: Inserting at "+myPort+" value ::"+values.toString());

/*
                    //Check whether it exists in the table
                    Cursor reader=getContext().getContentResolver().query(uri,null,keyOriginal,null,null);
                    if(reader == null || reader.getCount()==0)
                    {
                        rows = writingDb.insert(TABLE_NAME, "", values);
                        System.out.println("USER:: Inserting at "+myPort+" value ::"+values.toString());

                    }
                    else
                    {
                        String updateQuery ="UPDATE "+TABLE_NAME+" SET "+secondColumn+" ='"+valueOriginal+"' WHERE "+firstColumn+" = '"+keyOriginal+"';";
                        System.out.println("Update Query:"+updateQuery);
                        writingDb.execSQL(updateQuery);
                        System.out.println("USER:: Inserting at "+myPort+" value ::"+values.toString());

                    }
*/
                    Log.v("insert", values.toString());


                }
                else if(c1>0 && c3>0 && c2<0)
                {
                    //Store the key-value pair
                    //Check whether it exists in the table
                    System.out.println(" Insert  Condition 2");
                    rows = writingDb.insert(TABLE_NAME, "", values);

/*
                    Cursor reader=getContext().getContentResolver().query(uri,null,keyOriginal,null,null);
                    if(reader.getCount()==0)
                    {
                        rows = writingDb.insert(TABLE_NAME, "", values);
                    }
                    else
                    {
                        String updateQuery ="UPDATE "+TABLE_NAME+" SET "+secondColumn+" ='"+valueOriginal+"' WHERE "+firstColumn+" = '"+keyOriginal+"';";
                        System.out.println("Update Query:"+updateQuery);
                        writingDb.execSQL(updateQuery);
                    }
*/
                    System.out.println("USER:: Inserting at " + myPort + " value ::" + values.toString());
                    Log.v("insert", values.toString());


                }
                else if(c1<0)
                {
                    System.out.println(" Insert Condition 3");

                    //forward the key-value pair-- Forward to nextNode
                    String messageSe=keyOriginal+messagedelimiter+valueOriginal;
                    String requestMessage = "insertforward"+delimiter+useMyport+delimiter+messageSe;

                    Log.v("insertforward", values.toString());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestMessage, myPort);

                }
                else if(c1>0 && c3<0 )
                {
                    System.out.println(" Insert Condition 4");

                    //Store the key-value pair
                    //Check whether it exists in the table
                    rows = writingDb.insert(TABLE_NAME, "", values);

/*
                    Cursor reader=getContext().getContentResolver().query(uri,null,keyOriginal,null,null);
                    if(reader.getCount()==0)
                    {
                        rows = writingDb.insert(TABLE_NAME, "", values);
                    }
                    else
                    {
                        String updateQuery ="UPDATE "+TABLE_NAME+" SET "+secondColumn+" ='"+valueOriginal+"' WHERE "+firstColumn+" = '"+keyOriginal+"';";
                        System.out.println("Update Query:"+updateQuery);
                        writingDb.execSQL(updateQuery);
                    }
*/
                    System.out.println("USER:: Inserting at "+myPort+" value ::"+values.toString());
                    Log.v("insert", values.toString());


                }
                else
                {
                    System.out.println(" Insert Condition 5");

                    //forward the key-value pair-- Forward to nextNode
                    String messageSe=keyOriginal+messagedelimiter+valueOriginal;
                    String requestMessage = "insertforward"+delimiter+useMyport+delimiter+messageSe;
                    Log.v("insertforward", values.toString());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestMessage, myPort);
                }

            }catch(NoSuchAlgorithmException e)
            {
                e.printStackTrace();

            }catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        else if(joinStatus==false)
        {
            System.out.println("Join Status is false in Insert");
            try{
                //Check whether it exists in the table
                Cursor reader=getContext().getContentResolver().query(uri,null,keyOriginal,null,null);
                if(reader.getCount()==0)
                {
                    rows = writingDb.insert(TABLE_NAME, "", values);
                }
                else
                {
                    String updateQuery ="UPDATE "+TABLE_NAME+" SET "+secondColumn+" ='"+valueOriginal+"' WHERE "+firstColumn+" = '"+keyOriginal+"';";
                    System.out.println("Update Query:"+updateQuery);
                    writingDb.execSQL(updateQuery);
                }
                Log.v("insert", values.toString());

            }catch(Exception e)
            {
                e.printStackTrace();
            }

        }
        return uri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder)
    {
        System.out.println("USER::This is inside query function");
        Cursor readerRet=null;
        if(joinStatus==false)
        {

            System.out.println("USER::Join Status is false inside query");
            System.out.println("USER::Selection parameters are "+selection);
            //If joinStatus if false then return same on "*" and "@"
            if((selection.equals("\"*\"")) ||(selection.equals("\"@\"")) )
            {
                //Return all the key-value pairs stored in local AVD
                String getQuery = "SELECT * FROM "+ TABLE_NAME +";";
                System.out.println("USER:: The get query is "+getQuery);

                Cursor reader=null;
                try{
                    if(!(readingDb.isOpen()))
                        System.out.println("Readingdb is closed");

                    reader = readingDb.rawQuery(getQuery,selectionArgs);
                    readerRet=reader;
                    return reader;

                }catch(SQLiteException e){
                    e.printStackTrace();
                }catch(Exception e){
                    e.printStackTrace();
                }

                Log.v("query", selection);

            }
            else
            {
                //Retrieve what is being asked
                String getQuery = "SELECT * FROM "+ TABLE_NAME +" WHERE " + firstColumn + " = '" + selection +"';";
                System.out.println("USER:: The get query is "+getQuery);


                Cursor reader=null;
                try{
                    if(!(readingDb.isOpen()))
                        System.out.println("Readingdb is closed");
                    reader = readingDb.rawQuery(getQuery,selectionArgs);
                    readerRet=reader;
                    return reader;

                }catch(SQLiteException e){
                    e.printStackTrace();
                }catch(Exception e){
                    e.printStackTrace();
                }

                Log.v("query", selection);

            }
        }
        if(joinStatus==true)
        {
           System.out.println("Join Status is true inside query");
           System.out.println("USER::Selection parameters are "+selection);
           if((selection.equals("\"*\"")) || selection.contains("all"))
           {
               //Means return all key-value from DHT

               boolean checkMine=true;
               String sendingPort=null;

               if(selection.contains(delimiter))
               {
                   //means it is coming as a forward
                   checkMine=false;
                   String [] teselc = selection.split(delimiter);
                   sendingPort=teselc[1];
               }
               else
               {
                   checkMine=true;
                   sendingPort= myPort;
               }
               Cursor reader=null;
               int keyIndex;
               int valueIndex;
               String returnKey;
               String returnValue;
               try {
                   if (!(readingDb.isOpen()))
                       System.out.println("Readingdb is closed");

                   //Save * result from your DB into own global Hashmap
                   String getQuery = "SELECT * FROM " + TABLE_NAME + ";";
                   System.out.println("USER:: The get query is " + getQuery);

                   reader = readingDb.rawQuery(getQuery, selectionArgs);
                   readerRet = reader;

                   for (boolean item = reader.moveToFirst(); item; item = reader.moveToNext()) {
                       keyIndex = reader.getColumnIndex(KEY_FIELD);
                       valueIndex = reader.getColumnIndex(VALUE_FIELD);
                       returnKey = reader.getString(keyIndex);
                       returnValue = reader.getString(valueIndex);
                       //Save in global hashMap
                       AllResultsMap.put(returnKey, returnValue);
                   }
               }catch(Exception e)
               {
                   e.printStackTrace();
               }

               if(checkMine)
               {
                   //Means it is my query -- starting from me

                       //Now pass to others in the ring
                       String messS = "queryallinitial"+delimiter+sendingPort;
                       Log.v("SingleQuery", messS);
                       new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messS, myPort);

                       waitAllQuery=true;

                       while (true)
                       {
                           if(!(waitAllQuery))
                            break;
                       }

                       System.out.println("out of wait channel");
                       try
                       {
                           String[] columnNames = new String[]{firstColumn, secondColumn};
                           MatrixCursor makingCursor = new MatrixCursor(columnNames);
                           String allKey=null;
                           String allValue=null;
                           System.out.println("Using KeySet");
                           for(String key: AllResultsMap.keySet())
                           {
                               System.out.println(key + " :: " + AllResultsMap.get(key));
                               allKey = key;
                               allValue = AllResultsMap.get(key);
                               makingCursor.addRow(new Object[]{allKey, allValue});

                           }
                           //Clear the single results hashMap

                           Log.v("All Query back", "Success");
                           waitAllQuery = false;
                           AllResultsMap.clear();

                           //return the cursor
                           return makingCursor;
                       }
                       catch(Exception e)
                       {
                           e.printStackTrace();
                       }
               }
               else
               {
                   //means it came as a forward
                   //Now pass to others in the ring
                   String messS = "queryallinitial"+delimiter+sendingPort;
                   Log.v("SingleQuery", messS);
                   new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messS, myPort);

               }


           }
           else if((selection.equals("\"@\"")))
           {
               //Means return all key-value from local
               String getQuery = "SELECT * FROM "+ TABLE_NAME +";";
               System.out.println("USER:: The get query is "+getQuery);


               Cursor reader=null;
               try{
                   if(!(readingDb.isOpen()))
                       System.out.println("Readingdb is closed");
                   reader = readingDb.rawQuery(getQuery,selectionArgs);
                   readerRet=reader;
                   return reader;

               }catch(SQLiteException e){
                   e.printStackTrace();
               }catch(Exception e){
                   e.printStackTrace();
               }
               Log.v("query", selection);

           }
           else // Here single keys are being queried
           {
                int c1=0,c2=0,c3=0;
               String selections=null;
               String keyOriginalHash;
               String recvOriginalPort=null;
               boolean checkMine=false;
               //See if the current AVD would have saved the parameter
               try
               {
                   //First check if the selection parameter has queryDelimiter
                   if(selection.contains(delimiter))
                   {
                       String [] splitQuery= selection.split(delimiter);
                       selections = splitQuery[0].trim();
                       recvOriginalPort = splitQuery[1];

                       keyOriginalHash = genHash(selections);
                       c1 = selfHash.compareTo(keyOriginalHash);
                       c2=  selfHash.compareTo(previousNodehash);
                       c3 = previousNodehash.compareTo(keyOriginalHash);
                       checkMine=false;
                   }
                   else
                   {
                       keyOriginalHash  = genHash(selection);
                       selections=selection;
                       recvOriginalPort=myPort;
                       originalPortMap.put(selection,recvOriginalPort);
                       checkMine=true;

                       c1 = selfHash.compareTo(keyOriginalHash);
                       c2=  selfHash.compareTo(previousNodehash);
                       c3 = previousNodehash.compareTo(keyOriginalHash);
                       //Put into hashMap that I am the original port of the key got in selection
                   }


                   if(c1<0 && c3<0 && c2<0)
                   {

                        System.out.println("Query Condition 1");
                        //Had Stored the key-value pair --- Retrieve what is being asked

                           String getQuery = "SELECT * FROM "+ TABLE_NAME +" WHERE " + firstColumn + " = '" + selections +"';";
                           System.out.println("USER:: The get query is "+getQuery);

                           Cursor reader=null;
                           try
                           {
                               if(!(readingDb.isOpen()))
                                   System.out.println("Readingdb is closed");
                               reader = readingDb.rawQuery(getQuery,selectionArgs);
                               readerRet=reader;
                               Log.v("query", selection);

                           }
                           catch(SQLiteException e){
                               e.printStackTrace();
                           }catch(Exception e){
                               e.printStackTrace();
                           }

                       //Check if it is mine by checking variable checkMine
                       if(checkMine)
                       {
                           //true -- meaning I am the originator
                           Log.v("query", selection);
                           return reader;

                       }
                       else
                       {
                           //false -- I am not the originator but I have it saved in my DB
                           //           Now return to OriginalPort
                           //         with MessageType= singleQueryReturn
                           try
                           {
                               int keyIndex = reader.getColumnIndex(KEY_FIELD);
                               int valueIndex = reader.getColumnIndex(VALUE_FIELD);
                               reader.moveToFirst();
                               String returnKey = reader.getString(keyIndex);
                               String returnValue = reader.getString(valueIndex);
                               String newMessage= "singleQueryReturn"+delimiter+returnValue+delimiter+recvOriginalPort+delimiter+returnKey;
                               Log.v("Originator", returnValue);
                               new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage, myPort);
                           }catch(Exception e)
                           {
                               e.printStackTrace();
                           }

                       }
                   }
                   else if(c1>0 && c3>0 && c2<0)
                   {
                       System.out.println("Query Condition 2");
                       //Had Stored the key-value pair --- Retrieve what is being asked

                       String getQuery = "SELECT * FROM "+ TABLE_NAME +" WHERE " + firstColumn + " = '" + selections +"';";
                       System.out.println("USER:: The get query is "+getQuery);

                       Cursor reader=null;
                       try
                       {
                           if(!(readingDb.isOpen()))
                               System.out.println("Readingdb is closed");
                           reader = readingDb.rawQuery(getQuery,selectionArgs);
                           readerRet=reader;
                           Log.v("query", selection);

                       }
                       catch(SQLiteException e){
                           e.printStackTrace();
                       }catch(Exception e){
                           e.printStackTrace();
                       }
                       //Check if it is mine by checking variable checkMine
                       if(checkMine)
                       {
                           //true -- meaning I am the originator
                           Log.v("query", selection);

                           return reader;

                       }
                       else
                       {
                           //false -- I am not the originator but I have it saved in my DB
                           //           Now return to OriginalPort
                           //         with MessageType= singleQueryReturn
                           try
                           {
                               int keyIndex = reader.getColumnIndex(KEY_FIELD);
                               int valueIndex = reader.getColumnIndex(VALUE_FIELD);
                               reader.moveToFirst();
                               String returnKey = reader.getString(keyIndex);
                               String returnValue = reader.getString(valueIndex);
                               String newMessage= "singleQueryReturn"+delimiter+returnValue+delimiter+recvOriginalPort+delimiter+returnKey;
                               System.out.println("Sebd Message  is "+newMessage);
                               Log.v("Originator", returnValue);
                               new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage, myPort);
                           }catch(Exception e)
                           {
                               e.printStackTrace();
                           }

                       }


                   }
                   else if(c1>0 && c3<0 )
                   {
                       System.out.println("Query Condition 4");
                       //Had Stored the key-value pair --- Retrieve what is being asked

                       String getQuery = "SELECT * FROM "+ TABLE_NAME +" WHERE " + firstColumn + " = '" + selections +"';";
                       System.out.println("USER:: The get query is "+getQuery);

                       Cursor reader=null;
                       try
                       {
                           if(!(readingDb.isOpen()))
                               System.out.println("Readingdb is closed");
                           reader = readingDb.rawQuery(getQuery,selectionArgs);
                           readerRet=reader;
                           Log.v("query", selection);

                       }
                       catch(SQLiteException e){
                           e.printStackTrace();
                       }catch(Exception e){
                           e.printStackTrace();
                       }

                       //Check if it is mine by checking variable checkMine
                       if(checkMine)
                       {
                           //true -- meaning I am the originator
                           Log.v("query", selection);
                           return reader;

                       }
                       else
                       {
                           //false -- I am not the originator but I have it saved in my DB
                           //           Now return to OriginalPort
                           //         with MessageType= singleQueryReturn
                           try
                           {
                               int keyIndex = reader.getColumnIndex(KEY_FIELD);
                               int valueIndex = reader.getColumnIndex(VALUE_FIELD);
                               reader.moveToFirst();
                               String returnKey = reader.getString(keyIndex);
                               String returnValue = reader.getString(valueIndex);
                               String newMessage= "singleQueryReturn"+delimiter+returnValue+delimiter+recvOriginalPort+delimiter+returnKey;
                               Log.v("Returned", returnValue);
                               new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage, myPort);
                           }catch(Exception e)
                           {
                               e.printStackTrace();
                           }

                       }

                   }
                   else if(c1<0)
                   {
                       System.out.println("Query Condition 3");
                       //Had forwarded the key-value pair-- Forward Request to nextNode
                       if(checkMine)
                       {
                           //Means I am the originator of the query
                           String mSend = "SingleQuery"+delimiter+recvOriginalPort+delimiter+selections;
                           Log.v("SingleQuery", mSend);
                           new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);
                           waitSingleQuery=true;
                           while(true)
                           {
                               //keep Looping tll waitSingleQuery turns false
                               if(!(waitSingleQuery))
                                   break;
                           }

                           System.out.println("out of wait channel");
                           //Results have been stored in singleResultsMap
                           //Make a matrix cursor and return it
                           try {
                               String[] columnNames = new String[] { firstColumn,secondColumn };


                               String singleResultValue=singleResultsMap.get(selections);

                               MatrixCursor makingCursor = new MatrixCursor(columnNames);
                              makingCursor.addRow(new Object[] {selections,singleResultValue});

                               //Clear the single results hashMap
                               Log.v("SingleQuery back",selections);
                                waitSingleQuery=false;
                               singleResultsMap.clear();

                               //
                               return makingCursor;

                           }catch(Exception e)
                           {
                               e.printStackTrace();
                           }
                       }
                       else
                       {
                           //Means the query has been forwarded
                           String mSend = "SingleQuery"+delimiter+recvOriginalPort+delimiter+selections;
                           Log.v("singlequery", mSend);
                           new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                       }


                   }
                   else
                   {
                       System.out.println("Query Condition 5");
                       //Had forwarded the key-value pair-- Forward Request to nextNode
                       if(checkMine)
                       {
                           //Means I am the originator of the query
                           String mSend = "SingleQuery"+delimiter+recvOriginalPort+delimiter+selections;
                           Log.v("SingleQuery", mSend);
                           new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                           waitSingleQuery=true;
                           while(true)
                           {
                               //keep Looping tll waitSingleQuery turns false
                               if(!(waitSingleQuery))
                                   break;
                           }

                           //Results have been stored in singleResultsMap
                           //Make a matrix cursor and return it
                           try {
                               String[] columnNames = new String[] { firstColumn,secondColumn };


                               String singleResultValue=singleResultsMap.get(selections);

                               MatrixCursor makingCursor = new MatrixCursor(columnNames);
                               makingCursor.addRow(new Object[] {selections,singleResultValue});

                               //Clear the single results hashMap
                               Log.v("SingleQuery back",selections);
                               waitSingleQuery=false;
                               singleResultsMap.clear();

                               //
                               return makingCursor;

                           }catch(Exception e)
                           {
                               e.printStackTrace();
                           }
                       }
                       else
                       {
                           //Means the query has been forwarded
                           String mSend = "SingleQuery"+delimiter+recvOriginalPort+delimiter+selections;
                           Log.v("SingleQuery", mSend);
                           new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mSend, myPort);

                       }

                   }

               }catch(Exception e)
               {
                   e.printStackTrace();
               }


            }

        }
    return readerRet;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    //Reference: http://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    System.out.println("USER::: We are in Server Task");
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("USER:::Client Socket connected " + clientSocket.toString());
//                  BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                    ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
                    CreateMessage recvMessage = (CreateMessage) in.readObject();

                    if (recvMessage != null)
                    {
//                        System.out.println("USER:::String received " + recvMessage.message.toString());
                        //Convert received inline message to type of MessageClass

            //            System.out.println("USER:: SERVER TASK: Message received is "+recvMessage.sendPort);
                        System.out.println("USER:: SERVER TASK: Message received is "+recvMessage.messageType);
            //            System.out.println("USER:: SERVER TASK: Message received is " + recvMessage.message);

                        //String recvPort = recvMessage.sendPort;
                        String recvType = recvMessage.messageType;
                        //String messageRecv = recvMessage.message;

                        if(recvMessage.messageType.equals("join"))
                        {
                            String messageRecv = recvMessage.message;
                            String recvPort = recvMessage.sendPort;
                            handleJoin(recvPort,recvType,messageRecv);
                        }
                        if(recvMessage.messageType.equals("joinreply"))
                        {
                            String messageRecv = recvMessage.message;
                            String [] recvmessArray = messageRecv.split(delimiter);
                            System.out.println("Message received is "+recvMessage.messageType+"->"+recvMessage.sendPort+"-->"+recvMessage.message);
                            previousNode=((Integer.parseInt(recvmessArray[0]))*2);
                            nextNode =((Integer.parseInt(recvmessArray[1]))*2);

                            previousNodehash=genHash(Integer.toString((previousNode/2)));
                            nextNodehash=genHash(Integer.toString((nextNode/2)));
                            joinStatus=true;
                            System.out.println("USER:: Ring for me is"+previousNode+"-->"+myPort+"-->"+nextNode);

                        }
                        if(recvMessage.messageType.equals("insertforward"))
                        {
                            System.out.println("USER:: Recv Message in insertforward is "+recvMessage.message);
                            String [] messrec = recvMessage.message.split(messagedelimiter);
                            //Insert the valuesn
                            //First check if we were the originator of the Insert Query
                            Uri retUri;
                            ContentValues tempContentValues = new ContentValues();
                            tempContentValues.put(KEY_FIELD, messrec[0].trim());
                            tempContentValues.put(VALUE_FIELD, messrec[1].trim());
                            insert(dhtUri,tempContentValues);
                        }
                        if(recvMessage.messageType.equals("singleQueryReturn"))
                        {
                            String originalRecvport= recvMessage.originalPort;
                            singleResultsMap=recvMessage.sendHashMap;
                            waitSingleQuery=false;

                        }
                        if(recvMessage.messageType.equals("SingleQuery"))
                        {
                            String originalRecvport= recvMessage.originalPort;
                            String querySelection = recvMessage.message;
                            String queryCommand = querySelection+delimiter+originalRecvport;
                            query(dhtUri,null,queryCommand,null,null);
                        }
                        if(recvMessage.messageType.equals("queryallinitial"))
                        {

                            String originalRecvport= recvMessage.originalPort;
                            AllResultsMap = recvMessage.sendHashMap;
                            System.out.println("Message type is "+recvMessage.messageType+" and original port is "+originalRecvport+" and hashmap size is "+AllResultsMap.size());
                            if(originalRecvport.equals(myPort))
                            {
                                System.out.println("I received back my own query");
                                waitAllQuery=false;
                            }
                            else
                            {
                                System.out.println("Query and forward");
                                String querySelection = "all";
                                String queryCommand = querySelection+delimiter+originalRecvport;
                                query(dhtUri,null,queryCommand,null,null);
                            }
                        }
                        if(recvMessage.messageType.equals("deleteSingle"))
                        {

                            String originalRecvport= recvMessage.originalPort;
                            System.out.println("Message type is "+recvMessage.messageType+" and original port is "+originalRecvport+" and message is "+recvMessage.message);
                            if(originalRecvport.equals(myPort))
                            {
                                System.out.println("I received back my own query");
                                waitAllQuery=false;
                            }
                            else
                            {
                                System.out.println("Query and forward");

                                String queryCommand = recvMessage.message+delimiter+originalRecvport;
                                delete(dhtUri,queryCommand,null);
                            }
                        }
                        if(recvMessage.messageType.equals("allDelete"))
                        {

                            String originalRecvport= recvMessage.originalPort;
                            System.out.println("Message type is "+recvMessage.messageType+" and original port is "+originalRecvport);
                            if(originalRecvport.equals(myPort))
                            {
                                System.out.println("I received back my own query in all Delete");
                                waitAllQuery=false;
                            }
                            else
                            {
                                System.out.println("Query and forward");
                                String queryCommand = recvMessage.message+delimiter+originalRecvport;
                                delete(dhtUri,queryCommand,null);
                            }
                        }
                        if(recvMessage.messageType.equals("deleteSingleSuccess"))
                        {

                            String originalRecvport = recvMessage.originalPort;
                            System.out.println("Message type is " + recvMessage.messageType + " and original port is " + originalRecvport);
                            if (originalRecvport.equals(myPort)) {
                                System.out.println("I received back my own query in Single Delete");
                                waitSingleDelete= false;
                            }
                        }





                        }

                    in.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "Error creating socket on accepting/while listening on a port");
            }catch(ClassNotFoundException e)
            {
                e.printStackTrace();
                Log.e(TAG, "Class conversion not found");
            }catch (Exception e)
            {
                e.printStackTrace();
                Log.e(TAG, e.toString());

            }
            return null;
        } // End of Do in background task

        public void handleJoin(String recvPort,String recvType,String recvm)
        {
            System.out.println("Message received is "+recvPort);
            System.out.println("Message received is "+recvType);
            System.out.println("Message received is " + recvm);

            try
            {
                String newHash= genHash(recvPort);
                ringMap.put(newHash, recvPort);
                int finalindex=findIndex(newHash);
                keyList.add(finalindex, newHash);

                //Now assign prev and next
                for(int j=0;j<keyList.size();j++)
                {
                    String hashedKey = keyList.get(j);
                    String strPrevnext = addprevNext(newHash,j);
                    myMap.put(hashedKey, strPrevnext);
                }

                //Send the previous and next node hash to the port from which we received node request
                String recvPortHash= genHash(recvPort);
                String sendNodesarr = myMap.get(recvPortHash);
                joinStatus=true;

                //Update own next and previous node hash
                String ownhash= genHash(useMyport);
                String [] ownhashArr = myMap.get(ownhash).split(delimiter);
                previousNodehash=ownhashArr[0];
                nextNodehash = ownhashArr[1];
                previousNode= (Integer.parseInt(ringMap.get(previousNodehash))*2);
                nextNode = (Integer.parseInt(ringMap.get(nextNodehash))*2);
                try
                {
                    //Send to all currently connected ports - run over ringmap
                    for(String key: myMap.keySet())
                    {
                        //System.out.println(key + " :: " + myMap.get(key));
                        int convertPort = Integer.parseInt(ringMap.get(key));
                        if(Integer.toString(convertPort).trim().equals(useMyport))
                        {
                            continue;
                        }
                        else
                        {
                            String [] sendNodeshash = myMap.get(key).split(delimiter);
                            sendNodesarr = ringMap.get(sendNodeshash[0])+delimiter+ringMap.get(sendNodeshash[1]);

                            int convertPor=convertPort*2;

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), convertPor);
                            ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                            CreateMessage sendMessage = new CreateMessage("joinreply",useMyport,sendNodesarr);
                            System.out.println("Sending reply to "+convertPor+" with message"+sendNodesarr);
                            clientOut.writeObject(sendMessage);
                            clientOut.close();
                            socket.close();
                        }
                    }
                    System.out.println("USER:: Ring for me is"+previousNode+"-->"+useMyport+"-->"+nextNode);

                }catch(Exception e)
                {
                    e.printStackTrace();
                    Log.e(TAG, "Can't send to requester of joining the ring");
                }
              /*  System.out.println("THe value from hashmap is "+ringMap.get(keyHash));
                System.out.println("PREV NODE is "+previousNode  +" and NEXT NODE is  "+ nextNode+"\n\n");
*/

            }catch(NoSuchAlgorithmException e)
            {
                e.printStackTrace();
            }catch(Exception e)
            {
                e.printStackTrace();
            }



        }
        public int findIndex(String hashStr)
        {
            int counter=0,compareTo=0;
            int finalindex=0;
            for(counter=0;counter<keyList.size();counter++)
            {
                compareTo= keyList.get(counter).compareTo(hashStr);
                if(compareTo<0)
                {
                    //Means allhash < newHash
                    finalindex=counter+1;
                }
                else if(compareTo>0)
                {
                    finalindex=counter;
                    break;
                }

            }
            return finalindex;

        }

        public String addprevNext(String newHash,int index)
        {
            String prev="nothing";
            String next="nothing";
            int size=keyList.size();
            if(index==0)
            {
                //means it is the first element
                prev= keyList.get(size-1);
                next= keyList.get(index+1);
            }
            else if(index==size-1)
            {
                //Means it is the last index
                prev=keyList.get(index-1);
                next=keyList.get(0);
            }
            else
            {
                next= keyList.get(index+1);
                prev= keyList.get(index-1);
            }

            String returnStr=prev+delimiter+next;
            return returnStr;
        }

    }   //End of Server Task class

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try
            {
                System.out.println("Message to send is "+msgs[0]);
                System.out.println("USER::: We are in ClientTask");

                try
                {
                    String [] messageArray = msgs[0].split(delimiter);
                    System.out.println("Message type received is "+messageArray[0]);
                    //Sending depending on the type of message
                    if(messageArray[0].trim().equals("join"))
                    {
                        String sendPort="11108";
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendPort));

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());

                        CreateMessage sendMessage = new CreateMessage(messageArray[0],messageArray[1],messageArray[2]);
                        clientOut.writeObject(sendMessage);
                        clientOut.close();

                        socket.close();

                    }
                    if(messageArray[0].trim().equals("insertforward"))
                    {

                        /*String sendPort=ringMap.get(nextNodehash);
                        int sendPortin = ((Integer.parseInt(sendPort))*2);
                        */
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), nextNode);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        System.out.println("Message being sent to"+nextNode);
                        CreateMessage sendMessage = new CreateMessage(messageArray[0],messageArray[1],messageArray[2]);
            //            System.out.println("Message being sent to "+nextNode+" with message as "+sendMessage.message);
                        clientOut.writeObject(sendMessage);
                        clientOut.close();

                        socket.close();

                    }
                    if(messageArray[0].trim().equals("singleQueryReturn"))
                    {
                        //Return to the originator
                        System.out.println("Value to be returned via singleQueryReturn from client "+messageArray[0]+" next:"+messageArray[1]+" next "+messageArray[2]);
                        String returnPort= messageArray[2];
                        HashMap<String,String> sendMap = new HashMap<String,String>();
                        sendMap.put(messageArray[3],messageArray[1]);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(returnPort));

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
               //         System.out.println("Message being sent to"+nextNode);
                        CreateMessage sendMessage = new CreateMessage(messageArray[0],messageArray[2],sendMap);
              //          System.out.println("Message being sent to "+nextNode+" with message hasmap size as "+sendMessage.sendHashMap.size());
                        clientOut.writeObject(sendMessage);
                        clientOut.close();

                        socket.close();

                    }
                    if(messageArray[0].trim().equals("SingleQuery"))
                    {
                        //Return to the originator
                        System.out.println("Value to SingleQuery from client " + messageArray[0] + " next:" + messageArray[1] + " next " + messageArray[2]);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), nextNode);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        CreateMessage sendMessage = new CreateMessage(messageArray[0],myPort,messageArray[2],messageArray[1]);
                       // System.out.println("Message being sent to "+nextNode+" with message as "+sendMessage.message);
                        clientOut.writeObject(sendMessage);

                        clientOut.close();
                        socket.close();

                    }
                    if(messageArray[0].trim().equals("queryallinitial"))
                    {
                        //Return to the originator
                        System.out.println("Value to SingleQuery from client " + messageArray[0] + " next:" + messageArray[1]);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), nextNode);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        CreateMessage sendMessage = new CreateMessage(messageArray[0],messageArray[1],AllResultsMap);
                     //   System.out.println("Message being sent to "+nextNode+" with hashmap size as "+sendMessage.sendHashMap.size());
                        clientOut.writeObject(sendMessage);

                        clientOut.close();
                        socket.close();

                    }
                    if(messageArray[0].trim().equals("queryallforward"))
                    {
                        System.out.println("Value to SingleQuery from client " + messageArray[0] + " next:" + messageArray[1]);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), nextNode);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        CreateMessage sendMessage = new CreateMessage(messageArray[0],messageArray[1],AllResultsMap);
                   //     System.out.println("Message being sent to "+nextNode+" with hashmap size as "+sendMessage.sendHashMap.size());
                        clientOut.writeObject(sendMessage);

                        clientOut.close();
                        socket.close();

                    }
                    if(messageArray[0].trim().equals("deleteSingle"))
                    {

                        System.out.println("Value to SingleQuery from client " + messageArray[0] + " next:" + messageArray[1]+" next:" + messageArray[2]);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), nextNode);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());

                        CreateMessage sendMessage = new CreateMessage(messageArray[0],myPort,messageArray[2],messageArray[1]);
                 //       System.out.println("Message being sent to "+nextNode+" with message as "+sendMessage.message+ " and originalPort as "+sendMessage.originalPort);
                        clientOut.writeObject(sendMessage);

                        clientOut.close();
                        socket.close();

                    }
                    if(messageArray[0].trim().equals("allDelete"))
                    {

                        System.out.println("Value to SingleQuery from client " + messageArray[0] + " next:" + messageArray[1]);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), nextNode);

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());

                        CreateMessage sendMessage = new CreateMessage(messageArray[0],messageArray[1]);
               //         System.out.println("Message being sent to "+nextNode+" with message as "+sendMessage.message+ " and originalPort as "+sendMessage.originalPort);
                        clientOut.writeObject(sendMessage);

                        clientOut.close();
                        socket.close();

                    }
                    if(messageArray[0].trim().equals("deleteSingleSuccess"))
                    {

                        System.out.println("Value to SingleQuery from client " + messageArray[0] + " next:" + messageArray[1]);
                        String sendNode = messageArray[1];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendNode));

                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());

                        CreateMessage sendMessage = new CreateMessage(messageArray[0],messageArray[1]);
//                        System.out.println("Message being sent to "+nextNode+" with message as "+sendMessage.message+ " and originalPort as "+sendMessage.originalPort);
                        clientOut.writeObject(sendMessage);

                        clientOut.close();
                        socket.close();

                    }








                }catch(SocketException e)
                {
                    e.printStackTrace();
                    Log.e(TAG, "Can't send to coordinator to join the ring");
                }catch(Exception e)
                {
                    e.printStackTrace();
                    Log.e(TAG, "Can't send to coordinator to join the ring");
                }

            }catch (Exception e)
            {
                e.printStackTrace();
                Log.e(TAG, "ClientTask UnknownHostException");
            }

            return null;
        }

    }

    public static class CreateMessage implements Serializable
    {
        String sendPort;
        String message;
        String messageType;
        String originalPort;
        HashMap<String,String> sendHashMap;

        public  CreateMessage(String messageType,String sendPort,String message)
        {
            this.sendPort=sendPort;
            this.message=message;
            this.messageType=messageType;
        }
        //For Insert command
        public  CreateMessage(String messageType,String sendPort,String message,String originalPort)
        {
            this.sendPort=sendPort;
            this.message=message;
            this.messageType=messageType;
            this.originalPort=originalPort;
        }
        public  CreateMessage(String messageType,String originalPort,HashMap sendHashMap)
        {
            this.originalPort=originalPort;
            this.messageType=messageType;
            this.sendHashMap=sendHashMap;
        }
        public  CreateMessage(String messageType,String originalPort)
        {
            this.messageType=messageType;
            this.originalPort=originalPort;

        }



    }
}
