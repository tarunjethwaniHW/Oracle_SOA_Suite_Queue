package com.freddiemac.jms.service;

import com.freddiemac.jms.bpel.LoanQueueOrchestratorBpel;
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
 * LoanOrchestratorListener acts as the entry-point JMS activation listener (mirroring the Inbound JMS adapter).
 * It listens on jms/LoanRequestQueue, delegates request payload processing to the LoanQueueOrchestratorBpel class,
 * and replies back to the client on the response queue.
 */
public class LoanOrchestratorListener {

    private static final String REQUEST_QUEUE_JNDI = "jms/LoanRequestQueue";
    private static final String RESPONSE_QUEUE_JNDI = "jms/LoanResponseQueue";
    private static volatile boolean running = true;

    public static void main(String[] args) {
        System.out.println("=== Starting Loan Orchestrator (BPEL Sim) JMS Listener ===");
        
        Context jndiContext = null;
        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        MessageProducer producer = null;

        try {
            // 1. Obtain JNDI Context
            jndiContext = JndiHelper.getInitialContext();
            
            // 2. Lookup ConnectionFactory
            String cfJndi = JndiHelper.getConnectionFactoryJndi();
            System.out.println("[Orchestrator] Looking up ConnectionFactory: " + cfJndi);
            ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(cfJndi);
            
            // 3. Lookup Queues
            System.out.println("[Orchestrator] Looking up request queue: " + REQUEST_QUEUE_JNDI);
            Queue requestQueue = (Queue) jndiContext.lookup(REQUEST_QUEUE_JNDI);
            
            System.out.println("[Orchestrator] Looking up fallback response queue: " + RESPONSE_QUEUE_JNDI);
            Queue fallbackResponseQueue = (Queue) jndiContext.lookup(RESPONSE_QUEUE_JNDI);

            // 4. Create JMS Connection & Session
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumer = session.createConsumer(requestQueue);
            producer = session.createProducer(null);

            // Start connection to receive
            connection.start();
            System.out.println("[Orchestrator] Successfully listening on: " + REQUEST_QUEUE_JNDI);
            System.out.println("Press Ctrl+C to terminate the orchestrator.");

            // Register JVM shutdown hook to close loop gracefully
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n[Orchestrator] Shutdown signal received. Exiting...");
                running = false;
            }));

            // Instantiate simulated BPEL Orchestrator
            LoanQueueOrchestratorBpel bpelEngine = new LoanQueueOrchestratorBpel();

            // 5. Polling Loop
            while (running) {
                Message message = consumer.receive(1000);
                if (message == null) {
                    continue;
                }

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String requestXml = textMessage.getText();
                    String correlationId = textMessage.getJMSCorrelationID();
                    
                    System.out.println("\n[Orchestrator] Inbound JMS Activation triggered (equivalent to ConsumeLoanRequest JCA)");
                    System.out.println("[Orchestrator] Correlation ID: " + correlationId);

                    // Execute BPEL Orchestration workflow
                    String responseXml = bpelEngine.orchestrate(requestXml, correlationId);

                    // Target reply queue is either the JMSReplyTo header or fallback queue
                    Destination replyDestination = textMessage.getJMSReplyTo();
                    if (replyDestination == null) {
                        replyDestination = fallbackResponseQueue;
                    }

                    System.out.println("[Orchestrator] Outbound JCA Reply: Publishing LoanResponse back to " + replyDestination);
                    
                    // Publish response
                    TextMessage replyMessage = session.createTextMessage(responseXml);
                    replyMessage.setJMSCorrelationID(correlationId);
                    producer.send(replyDestination, replyMessage);
                    
                    System.out.println("[Orchestrator] Transaction complete.");
                } else {
                    System.out.println("[Orchestrator] Received non-TextMessage, ignoring.");
                }
            }

        } catch (Exception e) {
            System.err.println("[Orchestrator] Exception in Loan Orchestrator Listener: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up resources cleanly
            if (producer != null) { try { producer.close(); } catch (JMSException e) {} }
            if (consumer != null) { try { consumer.close(); } catch (JMSException e) {} }
            if (session != null) { try { session.close(); } catch (JMSException e) {} }
            if (connection != null) { try { connection.close(); } catch (JMSException e) {} }
            if (jndiContext != null) { try { jndiContext.close(); } catch (Exception e) {} }
            System.out.println("[Orchestrator] Loan Orchestrator stopped.");
        }
    }
}
