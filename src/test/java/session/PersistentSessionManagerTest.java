package session;

import database.NDCSCredsProviderForIAM;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.values.JsonOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import spock.lang.Specification;

import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.net.URL;
import java.util.Map;

import static session.PersistentSessionManager.TABLE_NAME;
import static session.PersistentSessionManager.COL_SESSION;

@MicronautTest
public class PersistentSessionManagerTest extends Specification {
    private static final String ENV_VAR_TEST_DATA_PATH_NAME = "path-to-test-data";

    @Inject
    static EmbeddedServer server = ApplicationContext.run(EmbeddedServer.class);
    HttpClient client = HttpClient.create(server.getURL());

    @BeforeAll
    public static void setup() throws Exception {
        Map<String, Object> props = server.getApplicationContext().getEnvironment().getProperties(
                "micronaut.test");
        NoSQLHandle dataStore = connect(props);
        PersistentSessionManager.createTable(dataStore);
        loadData(dataStore, props, "micronaut.test");
    }

    @Test
    public void testGetUsersInAccount() throws Exception {
        HttpRequest<String> request = HttpRequest.GET("/sessionmanager/getusers/3");
        String body = client.toBlocking().retrieve(request);
        JsonReader jsonReader = Json.createReader(new StringReader(body));
        JsonArray users = jsonReader.readArray();
        assert(users.size() == 2);
        users.forEach(user -> {
            int id = user.asJsonObject().getJsonObject(COL_SESSION).getInt("userID");
            assert ((id == 2001) || (id == 1002));
        });
    }
    @Test
    public void testGetSpecificUser() throws Exception {
        HttpRequest<String> request = HttpRequest.GET("/sessionmanager/getsession/2/2");
        String body = client.toBlocking().retrieve(request);
        JsonReader jsonReader = Json.createReader(new StringReader(body));
        JsonObject user = jsonReader.readObject();
        assert (user.getInt("userID") == 2);
    }

    @Test
    public void testUpdateSession() throws Exception {
        HttpRequest<String> request = HttpRequest.GET("/sessionmanager/getsession/3/2001");
        String body = client.toBlocking().retrieve(request);
        JsonReader jsonReader = Json.createReader(new StringReader(body));
        JsonObject user = jsonReader.readObject();
        //  Add a new episode to season 1

    }

    @AfterAll
    public static void cleanup() {
        server.stop();
    }

    private static void loadData(NoSQLHandle dataStore, Map<String, Object> props,
                                 String propsNamespace) throws Exception {

        String pathTOData = (String) props.get(ENV_VAR_TEST_DATA_PATH_NAME);
        File pathF = new File(pathTOData);
        if (!pathF.exists()) {
            throw new Exception("Unable to file " + pathTOData + " " +
                    "configured via " + propsNamespace + "." + ENV_VAR_TEST_DATA_PATH_NAME);
        }
        File[] datafiles = pathF.listFiles();
        for (File f : datafiles) {
            FileReader reader = new FileReader(f);
            char[] jsonText = new char[(int) f.length()];
            reader.read(jsonText, 0, (int) f.length());
            dataStore.put(new PutRequest().setTableName(TABLE_NAME).setValueFromJson(new String(jsonText),
                    new JsonOptions()));
        }
    }

    private static NoSQLHandle connect(Map<String, Object> props) throws Exception {
        NDCSCredsProviderForIAM creds = NDCSCredsProviderForIAM.getFromMap(props);
        URL serviceURL = new URL("https", creds.getRegionalURI(), 443, "/");
        NoSQLHandleConfig config = new NoSQLHandleConfig(serviceURL);

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
}
