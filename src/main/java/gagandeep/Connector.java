package gagandeep;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import java.nio.file.Paths;


public class Connector {

    private static final String SECURE_CONNECT_BUNDLE_PATH = "C:\\Users\\gagan\\OneDrive\\Desktop\\Courses\\BigD\\Cassandra Java Programming\\secure-connect-booksdb (1).zip";
    private static final String USERNAME = "zsSZpIxndoujhFjIrEzMBswu";
    private static final String PASSWORD = "HsUaPjLEdxH-C.QE2AQ4,BMIfudl83.Z+CrE0BviHz+-rdx9,OIjzDYwC54G_F+vrzWsQBHoPXbqW4L6rfZbWKcg7WDReZfG+BNpodxZS0y1p1h-_Q6duL3wkE-SR03j";
    private static final String KEYSPACE = "ks_books";

    public static CqlSession getSession() {
        CqlSession session = null;

        try {
            CqlSessionBuilder builder = CqlSession.builder();
            builder.withCloudSecureConnectBundle(Paths.get(SECURE_CONNECT_BUNDLE_PATH))
                   .withAuthCredentials(USERNAME, PASSWORD)
                   .withKeyspace(KEYSPACE);
            session = builder.build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return session;
    }

}

