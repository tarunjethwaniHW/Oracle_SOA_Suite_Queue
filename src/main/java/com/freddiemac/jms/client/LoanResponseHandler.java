package com.freddiemac.jms.client;

import java.util.Hashtable;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * LoanResponseHandler is responsible for receiving loan approval responses from the SOA composite.
 *
 * This class uses STANDARD JMS API methods for queue-based communication:
 * - InitialContext.lookup() - resolves queue JNDI
 * - Session.createConsumer() - creates consumer bound to queue
 * - MessageConsumer.receive() - receives message (CONSUMER SIGNAL for linking)
 *
 * Call chain: ProduceLoanResponse_jms.jca [JCA_ADAPTER_OUTBOUND] → waitForResponse()
 */
public class LoanResponseHandler {

    // WebLogic JNDI configuration
    private static final String INITIAL_CONTEXT_FACTORY = "weblogic.jndi.WLInitialContextFactory";
    private static final String PROVIDER_URL = "t3://localhost:7001";
    private static final String CONNECTION_FACTORY_JNDI = "eis/wls/Queue";

    // Queue JNDI name - must match JCA adapter DestinationName
    private static final String RESPONSE_QUEUE_JNDI = "jms/LoanResponseQueue";

    private Context jndiContext;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private boolean initialized = false;

    /**
     * Initialize JMS resources using standard JMS API.
     * Uses direct InitialContext creation without any wrapper classes.
     */
    public void initialize() throws NamingException, JMSException {
        // Standard JNDI InitialContext creation
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
        env.put(Context.PROVIDER_URL, PROVIDER_URL);

        jndiContext = new InitialContext(env);

        // Standard JNDI lookup for ConnectionFactory
        ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(CONNECTION_FACTORY_JNDI);

        // Standard JNDI lookup for Response Queue - FIXED pattern for queue resolution
        Queue responseQueue = (Queue) jndiContext.lookup(RESPONSE_QUEUE_JNDI);

        // Create JMS Connection and Session
        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Standard: Session.createConsumer(Destination) - FIXED pattern
        // This marks this method's containing snippet as JMS endpoint
        consumer = session.createConsumer(responseQueue);

        connection.start();
        initialized = true;
        System.out.println("[Handler] Initialized - Consumer bound to: " + RESPONSE_QUEUE_JNDI);
    }

    /**
     * Wait for and receive a loan response from the SOA composite via JMS queue.
     *
     * This method is the target for linking FROM:
     * ProduceLoanResponse_jms.jca [JCA_ADAPTER_OUTBOUND] via jmsQueueJndi match.
     *
     * STANDARD JMS API: MessageConsumer.receive(long) - CONSUMER SIGNAL
     *
     * @param correlationId The correlation ID to match the response
     * @param timeoutMs Maximum time to wait for response in milliseconds
     * @return The loan response XML, or null if timeout
     */
    public String waitForResponse(String correlationId, long timeoutMs) throws JMSException, NamingException {
        if (!initialized) {
            initialize();
        }

        System.out.println("[Handler] Waiting for response with correlation ID: " + correlationId);
        System.out.println("[Handler] Timeout: " + timeoutMs + "ms");

        // STANDARD JMS API: MessageConsumer.receive(long timeout)
        // This is the FIXED CONSUMER SIGNAL that triggers linking from JCA_ADAPTER_OUTBOUND
        TextMessage responseMessage = (TextMessage) consumer.receive(timeoutMs);

        if (responseMessage != null) {
            String responseXml = responseMessage.getText();
            String receivedCorrelationId = responseMessage.getJMSCorrelationID();

            System.out.println("[Handler] Response received!");
            System.out.println("[Handler] Correlation ID: " + receivedCorrelationId);
            System.out.println("[Handler] Response XML:\n" + responseXml);

            // Parse and display key response fields
            String status = extractElementValue(responseXml, "status");
            String remarks = extractElementValue(responseXml, "remarks");

            System.out.println("[Handler] Loan Status: " + status);
            System.out.println("[Handler] Remarks: " + remarks);

            return responseXml;
        } else {
            System.out.println("[Handler] Timeout - no response received for: " + correlationId);
            return null;
        }
    }

    /**
     * Receive response using correlation ID selector.
     * This variant uses a JMS message selector to filter by correlation ID.
     *
     * STANDARD JMS API: Session.createConsumer(Destination, String selector)
     * STANDARD JMS API: MessageConsumer.receive(long timeout)
     *
     * @param correlationId The correlation ID to filter on
     * @param timeoutMs Maximum time to wait
     * @return The loan response XML, or null if timeout
     */
    public String waitForResponseWithSelector(String correlationId, long timeoutMs) throws JMSException, NamingException {
        // Initialize JNDI context if needed
        if (jndiContext == null) {
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
            env.put(Context.PROVIDER_URL, PROVIDER_URL);
            jndiContext = new InitialContext(env);
        }

        // Lookup resources
        ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(CONNECTION_FACTORY_JNDI);
        Queue responseQueue = (Queue) jndiContext.lookup(RESPONSE_QUEUE_JNDI);

        Connection conn = null;
        Session sess = null;
        MessageConsumer selectorConsumer = null;

        try {
            conn = connectionFactory.createConnection();
            sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create consumer with message selector for correlation ID
            String selector = "JMSCorrelationID = '" + correlationId + "'";
            System.out.println("[Handler] Creating consumer with selector: " + selector);

            // STANDARD JMS API: Session.createConsumer(Destination, String selector)
            selectorConsumer = sess.createConsumer(responseQueue, selector);

            conn.start();

            // STANDARD JMS API: MessageConsumer.receive(long timeout)
            TextMessage responseMessage = (TextMessage) selectorConsumer.receive(timeoutMs);

            if (responseMessage != null) {
                String responseXml = responseMessage.getText();
                System.out.println("[Handler] Response received for correlation ID: " + correlationId);
                return responseXml;
            } else {
                System.out.println("[Handler] Timeout waiting for correlation ID: " + correlationId);
                return null;
            }
        } finally {
            if (selectorConsumer != null) try { selectorConsumer.close(); } catch (JMSException e) {}
            if (sess != null) try { sess.close(); } catch (JMSException e) {}
            if (conn != null) try { conn.close(); } catch (JMSException e) {}
        }
    }

    /**
     * Clean up JMS resources.
     */
    public void close() {
        try {
            if (consumer != null) consumer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
            if (jndiContext != null) jndiContext.close();
        } catch (Exception e) {
            System.err.println("[Handler] Error during cleanup: " + e.getMessage());
        }
        initialized = false;
        System.out.println("[Handler] Resources closed.");
    }

    private String extractElementValue(String xml, String elementName) {
        if (xml == null || elementName == null) return null;
        String openTagPattern = "<(?:[^>:]+:)?" + elementName + ">";
        String closeTagPattern = "</(?:[^>:]+:)?" + elementName + ">";
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                openTagPattern + "(.*?)" + closeTagPattern,
                java.util.regex.Pattern.DOTALL
        );
        java.util.regex.Matcher matcher = pattern.matcher(xml);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }
}
