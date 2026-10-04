package com.freddiemac.jms.service;

import com.freddiemac.jms.util.JndiHelper;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;

/**
 * CreditServiceListener simulates a downstream Credit Rating microservice.
 * It consumes request XML payloads from jms/CreditRequestQueue, evaluates the applicant's SSN,
 * calculates a mock credit score/risk, and returns a CreditResponse to the reply queue.
 * The response copies the original request's JMSCorrelationID to align with the BPEL Correlation Set.
 */
public class CreditServiceListener {

    private static final String REQUEST_QUEUE_JNDI = "jms/CreditRequestQueue";
    private static final String RESPONSE_QUEUE_JNDI = "jms/CreditResponseQueue";
    private static volatile boolean running = true;

    public static void main(String[] args) {
        System.out.println("=== Starting Credit Service JMS Listener ===");
        
        Context jndiContext = null;
        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        MessageProducer producer = null;

        try {
            // 1. Obtain JNDI InitialContext
            jndiContext = JndiHelper.getInitialContext();
            
            // 2. Lookup ConnectionFactory
            String cfJndi = JndiHelper.getConnectionFactoryJndi();
            System.out.println("[Service] Looking up ConnectionFactory: " + cfJndi);
            ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(cfJndi);
            
            // 3. Lookup Queues
            System.out.println("[Service] Looking up request queue: " + REQUEST_QUEUE_JNDI);
            Queue requestQueue = (Queue) jndiContext.lookup(REQUEST_QUEUE_JNDI);
            
            System.out.println("[Service] Looking up fallback response queue: " + RESPONSE_QUEUE_JNDI);
            Queue fallbackResponseQueue = (Queue) jndiContext.lookup(RESPONSE_QUEUE_JNDI);

            // 4. Create JMS Connection & Session
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // 5. Create Consumer on request queue and anonymous Producer
            consumer = session.createConsumer(requestQueue);
            producer = session.createProducer(null); // Destination will be specified per-message sent

            // Start connection to receive messages
            connection.start();
            System.out.println("[Service] Successfully listening on: " + REQUEST_QUEUE_JNDI);
            System.out.println("Press Ctrl+C to terminate the service.");

            // Register JVM shutdown hook to close loop gracefully
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n[Service] Shutdown signal received. Exiting...");
                running = false;
            }));

            // 6. Polling loop to consume messages
            while (running) {
                Message message = consumer.receive(1000); // 1-second timeout to allow graceful exit
                if (message == null) {
                    continue;
                }

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String requestXml = textMessage.getText();
                    String correlationId = textMessage.getJMSCorrelationID();
                    
                    System.out.println("\n[Service] Received Request with Correlation ID: " + correlationId);
                    System.out.println("[Service] XML Payload:\n" + requestXml);

                    // Parse request (SSN) using simple regex/string extraction helper
                    String ssn = extractElementValue(requestXml, "ssn");
                    System.out.println("[Service] Extracted SSN: " + ssn);

                    // Business Logic: SSNs starting with 999 are mock poor risk, others are good
                    int creditScore = 720;
                    String riskRating = "GOOD";
                    if (ssn != null && ssn.startsWith("999")) {
                        creditScore = 450;
                        riskRating = "POOR";
                    }

                    // Build CreditResponse XML
                    String responseXml = buildCreditResponseXml(correlationId, creditScore, riskRating);

                    // Target reply queue is either the JMSReplyTo header (standard practice) or fallback queue
                    Destination replyDestination = textMessage.getJMSReplyTo();
                    if (replyDestination == null) {
                        replyDestination = fallbackResponseQueue;
                    }

                    System.out.println("[Service] Publishing CreditResponse (Score=" + creditScore + ", Risk=" + riskRating + ") to: " + replyDestination);
                    
                    // Create and publish response message
                    TextMessage replyMessage = session.createTextMessage(responseXml);
                    replyMessage.setJMSCorrelationID(correlationId);
                    producer.send(replyDestination, replyMessage);
                    
                    System.out.println("[Service] Response sent successfully.");
                } else {
                    System.out.println("[Service] Received non-TextMessage: " + message.getClass().getName() + ", ignoring.");
                }
            }

        } catch (Exception e) {
            System.err.println("[Service] Exception in Credit Service Listener: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up resources cleanly
            if (producer != null) {
                try { producer.close(); } catch (JMSException e) { /* ignored */ }
            }
            if (consumer != null) {
                try { consumer.close(); } catch (JMSException e) { /* ignored */ }
            }
            if (session != null) {
                try { session.close(); } catch (JMSException e) { /* ignored */ }
            }
            if (connection != null) {
                try { connection.close(); } catch (JMSException e) { /* ignored */ }
            }
            if (jndiContext != null) {
                try { jndiContext.close(); } catch (Exception e) { /* ignored */ }
            }
            System.out.println("[Service] Credit Service shutdown finished.");
        }
    }

    private static String extractElementValue(String xml, String elementName) {
        if (xml == null || elementName == null) {
            return null;
        }
        // Matches <elementName>value</elementName> or with prefix <ns:elementName>value</ns:elementName>
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

    private static String buildCreditResponseXml(String correlationId, int score, String risk) {
        return "<types:CreditResponse xmlns:types=\"http://xmlns.oracle.com/pcbpel/adapter/jms/types\">\n" +
               "  <types:transactionId>" + correlationId + "</types:transactionId>\n" +
               "  <types:creditScore>" + score + "</types:creditScore>\n" +
               "  <types:riskRating>" + risk + "</types:riskRating>\n" +
               "</types:CreditResponse>";
    }
}
