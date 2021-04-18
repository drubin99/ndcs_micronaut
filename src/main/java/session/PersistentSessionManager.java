package session;

import database.NDCSCredsProviderForIAM;
import example.micronaut.Application;
import io.micronaut.context.env.Environment;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.runtime.Micronaut;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;

import javax.json.Json;
import javax.json.JsonMergePatch;
import javax.json.JsonReader;
import java.io.File;
import java.io.StringReader;
import java.net.URL;
import java.util.Map;

@Controller("/sessionmanager")
public class PersistentSessionManager implements ApplicationEventListener<ServerStartupEvent> {

    //  Constants for the name of the table and it's columns
    public static String TABLE_NAME = "persistent_session";
    public static String COL_ACCOUNT_NUMBER = "account_number";
    public static String COL_USER_ID = "user_id";
    public static String COL_SESSION = "session_info";
    public static String COL_GENERATED_ID_TYPE = " INTEGER GENERATED ALWAYS AS IDENTITY";
    public static String COL_JSON_TYPE = " JSON";
    public static String COL_LONG_TYPE = " LONG";

    //  Constants for attributes in the schema-less JSON part of the table
    public static String JSON_ATTR_USER_NAME = "userName";

    //  The number of reads, writes, and GB storage we will create out table with
    public static int DEFAULT_READS_SEC = 25;
    public static int DEFAULT_WRITES_SEC = 25;
    public static int DEFAULT_STORAGE_GB = 5;

    //  TCP connection pool size
    public static String ENV_PROPERTY_MAX_CONCURRENCY = "max_concurrency";
    public static int DEFAULT_MAX_CONCURRENCY = 10;

    //  The connection to the Oracle NoSQL Database
    static NoSQLHandle databaseConnection;

    public static void main(String args[]) {
        try {
            Micronaut.run(Application.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Initialize a connection to the Oracle NOSQL Database.
     * @param event Passed to us by Micronaut
     */
    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        try {
            System.out.println("Initializing environment");
            //  Our configuration for connecting to the service will be in this environment
            Environment env = event.getSource().getApplicationContext().getEnvironment();
            databaseConnection = connectToNDCS(env);
            createTable(databaseConnection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Using the configuration information from the environment, authenticate with the Oracle
     * NoSQL Database cloud service.
     *
     * @param env The environment to use for configuration information
     * @return An active handle to the cloud service
     * @throws Exception on error
     */
    private static NoSQLHandle connectToNDCS(Environment env) throws Exception {
        Map<String, Object> configProperties = env.getProperties("micronaut.application");
        // Grab all credentials needed for authentiation from our configuration env
        NDCSCredsProviderForIAM creds = NDCSCredsProviderForIAM.getFromMap(configProperties);
        URL serviceURL = new URL("https", creds.getRegionalURI(), 443, "/");
        NoSQLHandleConfig config = new NoSQLHandleConfig(serviceURL);
        int concurrency = (configProperties.containsKey(ENV_PROPERTY_MAX_CONCURRENCY) ?
                (int) configProperties.get(ENV_PROPERTY_MAX_CONCURRENCY) :
                        DEFAULT_MAX_CONCURRENCY);
        config.setConnectionPoolSize(concurrency);

        SignatureProvider authProvider = new SignatureProvider(creds.getTenantOCID(),
                creds.getUserOCID(),
                creds.getFingerprint(),
                new File(creds.getPathToSKFile()),
                creds.getSkPassword());
        config.setRequestTimeout(15000);
        config.setAuthorizationProvider(authProvider);
        config.configureDefaultRetryHandler(1, 10);
        config.setDefaultCompartment(creds.getCompartment());
        return(NoSQLHandleFactory.createNoSQLHandle(config));
    }

    /**
     *  Create the table that we will use for our persistent sessions.  This method is resilient in
     *  that if the table already exists, there is no harm in calling it.  Nothing will happen.
     *
     * @param dbHandle A handle to the NoSQL Database cloud service where the table will be
     *                 created.  NOTE that whatever compartment has been set on this handle will
     *                 be the target compartment for this table.
     * @throws Exception on error
     */
    public static void createTable(NoSQLHandle dbHandle) throws Exception {
        TableRequest req = new TableRequest().setStatement(
                "CREATE TABLE if not exists " +
                        TABLE_NAME + "(" +
                        COL_ACCOUNT_NUMBER + COL_LONG_TYPE + " , " +
                        COL_USER_ID + COL_GENERATED_ID_TYPE + " , " +
                        COL_SESSION + COL_JSON_TYPE + " , " +
                        "PRIMARY KEY (" + COL_ACCOUNT_NUMBER +", " + COL_USER_ID + "))");

        req.setTableLimits(new TableLimits(DEFAULT_READS_SEC,
                DEFAULT_WRITES_SEC,
                DEFAULT_STORAGE_GB));

        TableResult tr = dbHandle.tableRequest(req);

        //  Wait for a max of 30 seconds for the table to become active, checking every 500
        //  milliseconds
        tr.waitForCompletion(dbHandle, 30000, 500);
        if (tr.getTableState().compareTo(TableResult.State.ACTIVE) != 0) {
            throw new Exception("Unable to create table  " + TABLE_NAME + ", current state = " + tr.getTableState());
        }
    }

    /**
     * Update the provisioned throughput or storage for the table.
     *
     * @param readUnits If not null, contains the new number of read units to set
     * @param writeUnits If not null, contains the new number of write units to set
     * @param storageGB If not null, contains the new number of gigibytes of storage to set
     */
    @Post
    public void updateTableLimits(@PathVariable @io.micronaut.core.annotation.Nullable Integer readUnits,
                                  @PathVariable @io.micronaut.core.annotation.Nullable Integer writeUnits,
                                  @PathVariable @io.micronaut.core.annotation.Nullable Integer storageGB) {
        //  Get the existing limits on the table
        GetTableRequest getReq = new GetTableRequest().setTableName(TABLE_NAME);
        TableResult result = databaseConnection.getTable(getReq);
        TableLimits limits = result.getTableLimits();

        //  Set the caller's new limits
        limits.setReadUnits(readUnits != null ? readUnits : limits.getReadUnits());
        limits.setWriteUnits(writeUnits != null ? writeUnits : limits.getWriteUnits());
        limits.setStorageGB(storageGB != null ? storageGB : limits.getStorageGB());

        //  Modify the table with the new limits object
        TableRequest tReq = new TableRequest().setTableLimits(limits);
        TableResult tRes = databaseConnection.tableRequest(tReq);
        //  Wait 2 seconds for operation to complete, checking every 300 milliseconds
        tRes.waitForCompletion(databaseConnection, 2000, 300);
    }

    /**
     *  REST interface to retrieve the persistent session for a specific user in a specific account
     *
     * @param accountNum The account number for the account that this user belongs to
     * @param userID The unique ID for the user whose persistent session to retrieve
     * @return The JSON session document
     */
    @Get(uri="/getsession/{accountNum}/{userID}", produces = MediaType.APPLICATION_JSON)
    public String getSessionForUser(@PathVariable Long accountNum, @PathVariable Integer userID) {
        return(getByPK(accountNum, userID));
    }

    /**
     * REST interface to retrieve all of the user names and IDs registered in a
     * specific account
     *
     * @param accountNum The account number to find users for
     * @return A JSON array containing documents of the form: {"userName" : "julie", "userID": 28}
     */
    @Get(uri="/getusers/{accountNum}", produces = MediaType.APPLICATION_JSON)
    public String getUsersInAccount(@PathVariable Long accountNum) {
        QueryRequest qr = new QueryRequest().setStatement(
                "select {\"userName\": p.session_info.userName," +
                        "\"userID\": p.user_id} as session_info from " +
                        TABLE_NAME + " p " +
                        " where p." + COL_ACCOUNT_NUMBER + " = " + accountNum);

        QueryResult res = databaseConnection.query(qr);
        //  Use the NoSQL ArrayValue convenience API for storing out results for easy conversion
        //  to a JSON string for our return value
        ArrayValue ret = new ArrayValue();
        res.getResults().forEach(result -> {
            ret.add(result);
        });
        return(ret.toJson());
    }

    /**
     *  Create a persistent session for a user in an account.
     *
     * @param accountNum The account number that this user belongs to
     * @param userName The name associated with this user
     * @return The ID of the user that was just created
     */
    @Post(uri = "/create/{accountNum}/{userName}", produces = MediaType.APPLICATION_JSON)
    public String create(@PathVariable Long accountNum, @PathVariable String userName) {
        PutRequest putReq =
                new PutRequest().setTableName(TABLE_NAME).setValue(
                        new MapValue().put(COL_ACCOUNT_NUMBER, accountNum).put(COL_SESSION,
                                new MapValue().put(JSON_ATTR_USER_NAME, userName)));
        PutResult putRes = databaseConnection.put(putReq);
        int newUserId = putRes.getGeneratedValue().asInteger().getValue();
        return ("{\"userID\":\"" + newUserId + "\"}");
    }

    /**
     * Update a persistent session by applying RFC 7386 json merge patch
     * @param jsonMerge The JSON merge patch to apply
     * @param accountNum The account number owning the session
     * @param userID The ID of the user in the account that this session belongs to
     * @return Merged JSON update
     */
    @Post(uri = "/update/{accountNum}/{userName}", produces = MediaType.APPLICATION_JSON)
    public String updateSession(@Body String jsonMerge, @PathVariable Long accountNum,
                                @PathVariable Integer userID) {

        /*
          Use JSON merge patch to merge the incoming changes in the persistent session (in the body
          of the HTTP request) with the persistent session in the database.
         */
        JsonReader jsonReader = Json.createReader(new StringReader(getByPK(accountNum, userID)));
        JsonMergePatch mergePatch =
                Json.createMergePatch(jsonReader.readValue());
        jsonReader = Json.createReader(new StringReader(jsonMerge));
        String mergeResult = mergePatch.apply(jsonReader.readValue()).toString();

        putByPK(accountNum, userID, mergeResult);
        return(mergeResult);
    }

    /**
     * Helper method to retrieve a persistent session by primary key.  A primary for a persisten
     * session is composed of (accountNum, userID)
     *
     * @param accountNum The account number for the session to retrieve
     * @param userID The user ID for the session within the account
     * @return
     */
    private String getByPK(long accountNum, int userID) {
        GetRequest gr = new GetRequest().setTableName(TABLE_NAME).
                setKey(new MapValue().put(COL_ACCOUNT_NUMBER, accountNum).put(COL_USER_ID,
                        userID)).setConsistency(Consistency.EVENTUAL);
        GetResult res = databaseConnection.get(gr);
        return(res.getValue().get(COL_SESSION).toJson(new JsonOptions()));
    }

    /**
     *  Helper method to save a persistent session by primary key.  A primary for a persistent
     *  session is composed of (accountNum, userID)
     *
     * @param accountNum The account number for the session to store
     * @param userID The user ID for the session within the account
     * @param jsonRecord The JSON persistent session record
     */
    private void putByPK(long accountNum, int userID, String jsonRecord) {
        PutRequest putReq =
                new PutRequest().setTableName(TABLE_NAME).setValue(
                        new MapValue().put(COL_ACCOUNT_NUMBER, accountNum).put(COL_USER_ID,
                                userID).put(COL_SESSION, jsonRecord));

        PutResult putRes = databaseConnection.put(putReq);
    }
}
