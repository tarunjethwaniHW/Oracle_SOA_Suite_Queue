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
 * - Session.createConsumer() - creates consumer bound to queue (tracks consumer → queue mapping)
 * - MessageConsumer.receive() - receives message (CONSUMER SIGNAL - marks method as JMS endpoint)
 *
 * Call chain: ProduceLoanResponse_jms.jca [JCA_ADAPTER_OUTBOUND] → waitForResponse()
 *
 * The parser now detects BOTH:
 * - createConsumer(queue) → tracks consumer variable → queue JNDI mapping
 * - consumer.receive() → marks the containing method as JMS endpoint using the tracked queue JNDI
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
     *
     * The createConsumer() call here tracks the consumer → queue JNDI mapping:
     *   _jmsConsumerQueues["consumer"] = "jms/LoanResponseQueue"
     *
     * This mapping is used later when consumer.receive() is detected in waitForResponse().
     */
    public void initialize() throws NamingException, JMSException {
        // Standard JNDI InitialContext creation
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
        env.put(Context.PROVIDER_URL, PROVIDER_URL);

        jndiContext = new InitialContext(env);

        // Standard JNDI lookup for ConnectionFactory
        ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(CONNECTION_FACTORY_JNDI);

        // Standard JNDI lookup for Response Queue - FIXED pattern
        Queue responseQueue = (Queue) jndiContext.lookup(RESPONSE_QUEUE_JNDI);

        // Create JMS Connection and Session
        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // STANDARD JMS API: Session.createConsumer(Destination)
        // This tracks: _jmsConsumerQueues["consumer"] = "jms/LoanResponseQueue"
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
     * STANDARD JMS API: MessageConsumer.receive(long timeout)
     * The parser detects consumer.receive() and:
     * 1. Looks up _jmsConsumerQueues["consumer"] → "jms/LoanResponseQueue"
     * 2. Marks THIS method (waitForResponse) with:
     *    - isJmsEndpoint = true
     *    - jmsQueueJndi = "jms/LoanResponseQueue"
     *
     * @param correlationId The correlation ID to match the response
     * @param timeoutMs Maximum time to wait for response in milliseconds
     * @return The loan response XML, or null if timeout
     */
    public String waitForResponse(String correlationId, long timeoutMs) throws JMSException, NamingException {
        if (!initialized) {
            initialize();
        }

        System.out.println("[Handler] Waiting for response on " + RESPONSE_QUEUE_JNDI);
        System.out.println("[Handler] Correlation ID: " + correlationId);
        System.out.println("[Handler] Timeout: " + timeoutMs + "ms");

        // STANDARD JMS API: MessageConsumer.receive(long timeout)
        // This is the FIXED CONSUMER SIGNAL that triggers:
        // 1. Lookup consumer variable → queue JNDI from _jmsConsumerQueues
        // 2. Mark this method as JMS endpoint for linking from JCA_ADAPTER_OUTBOUND
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
