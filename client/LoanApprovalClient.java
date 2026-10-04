package com.freddiemac.jms.client;

import com.freddiemac.jms.util.JndiHelper;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;

/**
 * LoanApprovalClient is a Java application that initiates a loan approval flow by
 * publishing a LoanRequest XML to the inbound queue and listening for the final response
 * from the composite orchestration engine matching the generated JMS correlation ID.
 */
public class LoanApprovalClient {

    private static final String REQUEST_QUEUE_JNDI = "jms/LoanRequestQueue";
    private static final String RESPONSE_QUEUE_JNDI = "jms/LoanResponseQueue";

    public static void main(String[] args) {
        System.out.println("=== Starting JMS Loan Approval Client ===");
        
        // Parse input parameters or fallback to defaults
        String ssn = args.length > 0 ? args[0] : "123-456-7890";
        String applicantName = args.length > 1 ? args[1] : "John Doe";
        double requestedAmount = args.length > 2 ? Double.parseDouble(args[2]) : 50000.0;
        double monthlyIncome = args.length > 3 ? Double.parseDouble(args[3]) : 6000.0;

        Context jndiContext = null;
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        MessageConsumer consumer = null;

        try {
            // 1. Obtain JNDI InitialContext
            jndiContext = JndiHelper.getInitialContext();
            
            // 2. Lookup ConnectionFactory
            String cfJndi = JndiHelper.getConnectionFactoryJndi();
            System.out.println("[Client] Looking up ConnectionFactory: " + cfJndi);
            ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(cfJndi);
            
            // 3. Lookup Queues
            System.out.println("[Client] Looking up request queue: " + REQUEST_QUEUE_JNDI);
            Queue requestQueue = (Queue) jndiContext.lookup(REQUEST_QUEUE_JNDI);
            
            System.out.println("[Client] Looking up response queue: " + RESPONSE_QUEUE_JNDI);
            Queue responseQueue = (Queue) jndiContext.lookup(RESPONSE_QUEUE_JNDI);

            // 4. Create JMS Connection & Session
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // Start connection to enable message delivery
            connection.start();

            // 5. Create Correlation ID and XML payload
            String correlationId = "TX-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            String requestXml = buildLoanRequestXml(correlationId, ssn, applicantName, requestedAmount, monthlyIncome);

            System.out.println("\n[Client] Generated JMSCorrelationID: " + correlationId);
            System.out.println("[Client] Request XML payload:\n" + requestXml);

            // 6. Send Request Message
            producer = session.createProducer(requestQueue);
            TextMessage requestMessage = session.createTextMessage(requestXml);
            requestMessage.setJMSCorrelationID(correlationId);
            requestMessage.setJMSReplyTo(responseQueue); // Explicitly state where reply should be posted
            
            producer.send(requestMessage);
            System.out.println("[Client] Message sent successfully!");

            // 7. Receive Response matching Correlation ID selector
            String selector = "JMSCorrelationID = '" + correlationId + "'";
            System.out.println("[Client] Creating consumer on " + RESPONSE_QUEUE_JNDI + " with selector: " + selector);
            consumer = session.createConsumer(responseQueue, selector);

            System.out.println("[Client] Waiting for orchestrated response (30 seconds timeout)...");
            TextMessage responseMessage = (TextMessage) consumer.receive(30000); // 30s timeout

            if (responseMessage != null) {
                String responseXml = responseMessage.getText();
                System.out.println("\n[Client] Received Response Message:\n" + responseXml);
                System.out.println("[Client] Verified Correlation ID: " + responseMessage.getJMSCorrelationID());
            } else {
                System.out.println("\n[Client] Timeout reached. No response received for correlation ID: " + correlationId);
            }

        } catch (Exception e) {
            System.err.println("[Client] Error running JMS flow: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up resources cleanly
            closeJmsResource(producer);
            closeJmsResource(consumer);
            if (session != null) {
                try { session.close(); } catch (JMSException e) { /* ignored */ }
            }
            if (connection != null) {
                try { connection.close(); } catch (JMSException e) { /* ignored */ }
            }
            if (jndiContext != null) {
                try { jndiContext.close(); } catch (Exception e) { /* ignored */ }
            }
            System.out.println("[Client] Client shutdown finished.");
        }
    }

    private static String buildLoanRequestXml(String correlationId, String ssn, String name, double amount, double income) {
        return "<types:LoanRequest xmlns:types=\"http://xmlns.oracle.com/pcbpel/adapter/jms/types\">\n" +
               "  <types:transactionId>" + correlationId + "</types:transactionId>\n" +
               "  <types:ssn>" + ssn + "</types:ssn>\n" +
               "  <types:applicantName>" + name + "</types:applicantName>\n" +
               "  <types:requestedAmount>" + amount + "</types:requestedAmount>\n" +
               "  <types:monthlyIncome>" + income + "</types:monthlyIncome>\n" +
               "</types:LoanRequest>";
    }

    private static void closeJmsResource(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }
}
