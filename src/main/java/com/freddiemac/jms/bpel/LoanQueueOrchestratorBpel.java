package com.freddiemac.jms.bpel;

import com.freddiemac.jms.util.JndiHelper;
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
 * LoanQueueOrchestratorBpel simulates the WS-BPEL orchestration engine logic.
 * It coordinates the asynchronous integration logic by invoking the Credit check queue
 * and waiting for the correlated credit response before preparing the final loan response.
 */
public class LoanQueueOrchestratorBpel {

    private static final String CREDIT_REQUEST_QUEUE = "jms/CreditRequestQueue";
    private static final String CREDIT_RESPONSE_QUEUE = "jms/CreditResponseQueue";

    /**
     * Orchestrates the workflow: sends request to Credit Service and waits for response
     * using the correlation ID selector, then outputs a consolidated reply XML.
     *
     * @param loanRequestXml XML string from client
     * @param correlationId unique tracking ID
     * @return LoanResponse XML string
     * @throws Exception if JMS operations fail
     */
    public String orchestrate(String loanRequestXml, String correlationId) throws Exception {
        System.out.println("[BPEL] Starting orchestration workflow for ID: " + correlationId);
        
        // 1. Extract values from LoanRequest XML
        String ssn = extractElementValue(loanRequestXml, "ssn");
        String applicantName = extractElementValue(loanRequestXml, "applicantName");
        String requestedAmountStr = extractElementValue(loanRequestXml, "requestedAmount");
        double requestedAmount = requestedAmountStr != null ? Double.parseDouble(requestedAmountStr) : 0.0;
        String monthlyIncomeStr = extractElementValue(loanRequestXml, "monthlyIncome");
        double monthlyIncome = monthlyIncomeStr != null ? Double.parseDouble(monthlyIncomeStr) : 0.0;

        System.out.println("[BPEL] Loan Request Extracted Data: SSN=" + ssn + ", Applicant=" + applicantName + ", Amount=" + requestedAmount);

        // 2. Prepare Credit Request XML
        String creditRequestXml = buildCreditRequestXml(correlationId, ssn, monthlyIncome);

        Context jndiContext = null;
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        MessageConsumer consumer = null;
        
        try {
            jndiContext = JndiHelper.getInitialContext();
            
            String cfJndi = JndiHelper.getConnectionFactoryJndi();
            ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(cfJndi);
            
            Queue requestQueue = (Queue) jndiContext.lookup(CREDIT_REQUEST_QUEUE);
            Queue responseQueue = (Queue) jndiContext.lookup(CREDIT_RESPONSE_QUEUE);

            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // Start connection so we can receive
            connection.start();

            // 3. Publish Credit Request message
            System.out.println("[BPEL] Invoking Credit Service Reference on " + CREDIT_REQUEST_QUEUE);
            producer = session.createProducer(requestQueue);
            TextMessage requestMsg = session.createTextMessage(creditRequestXml);
            requestMsg.setJMSCorrelationID(correlationId);
            requestMsg.setJMSReplyTo(responseQueue);
            producer.send(requestMsg);

            // 4. Wait for Credit Response using Selector (BPEL Correlation Set simulation)
            String selector = "JMSCorrelationID = '" + correlationId + "'";
            System.out.println("[BPEL] Waiting for Credit Response on " + CREDIT_RESPONSE_QUEUE + " using selector: " + selector);
            consumer = session.createConsumer(responseQueue, selector);
            
            // Wait up to 15 seconds
            TextMessage responseMsg = (TextMessage) consumer.receive(15000);

            if (responseMsg != null) {
                String creditResponseXml = responseMsg.getText();
                System.out.println("[BPEL] Received Credit Response:\n" + creditResponseXml);
                
                String scoreStr = extractElementValue(creditResponseXml, "creditScore");
                int score = scoreStr != null ? Integer.parseInt(scoreStr) : 0;
                String risk = extractElementValue(creditResponseXml, "riskRating");
                
                // 5. Evaluate and Assign final loan status (equivalent to BPEL switch/assign)
                String status = "DENIED";
                double interestRate = 0.0;
                double maxApprovedLimit = 0.0;
                String remarks;

                if (score >= 500) {
                    status = "APPROVED";
                    interestRate = 0.045; // 4.5%
                    maxApprovedLimit = requestedAmount;
                    remarks = "Credit score " + score + " (" + risk + ") is sufficient. Loan approved.";
                } else {
                    remarks = "Credit score " + score + " (" + risk + ") is too low. Loan denied.";
                }

                return buildLoanResponseXml(correlationId, applicantName, status, interestRate, maxApprovedLimit, remarks);
            } else {
                System.err.println("[BPEL] Timeout waiting for Credit Response!");
                return buildLoanResponseXml(correlationId, applicantName, "DENIED", 0.0, 0.0, "Credit check service timeout.");
            }

        } finally {
            if (producer != null) { try { producer.close(); } catch (JMSException e) {} }
            if (consumer != null) { try { consumer.close(); } catch (JMSException e) {} }
            if (session != null) { try { session.close(); } catch (JMSException e) {} }
            if (connection != null) { try { connection.close(); } catch (JMSException e) {} }
            if (jndiContext != null) { try { jndiContext.close(); } catch (Exception e) {} }
        }
    }

    private static String extractElementValue(String xml, String elementName) {
        if (xml == null || elementName == null) return null;
        String openTagPattern = "<(?:[^>:]+:)?" + elementName + ">";
        String closeTagPattern = "</(?:[^>:]+:)?" + elementName + ">";
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(openTagPattern + "(.*?)" + closeTagPattern, java.util.regex.Pattern.DOTALL);
        java.util.regex.Matcher matcher = pattern.matcher(xml);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    private static String buildCreditRequestXml(String correlationId, String ssn, double income) {
        return "<types:CreditRequest xmlns:types=\"http://xmlns.oracle.com/pcbpel/adapter/jms/types\">\n" +
               "  <types:transactionId>" + correlationId + "</types:transactionId>\n" +
               "  <types:ssn>" + ssn + "</types:ssn>\n" +
               "  <types:monthlyIncome>" + income + "</types:monthlyIncome>\n" +
               "</types:CreditRequest>";
    }

    private static String buildLoanResponseXml(String correlationId, String name, String status, double rate, double limit, String remarks) {
        return "<types:LoanResponse xmlns:types=\"http://xmlns.oracle.com/pcbpel/adapter/jms/types\">\n" +
               "  <types:transactionId>" + correlationId + "</types:transactionId>\n" +
               "  <types:applicantName>" + name + "</types:applicantName>\n" +
               "  <types:status>" + status + "</types:status>\n" +
               "  <types:interestRate>" + rate + "</types:interestRate>\n" +
               "  <types:maxApprovedLimit>" + limit + "</types:maxApprovedLimit>\n" +
               "  <types:remarks>" + remarks + "</types:remarks>\n" +
               "</types:LoanResponse>";
    }
}
