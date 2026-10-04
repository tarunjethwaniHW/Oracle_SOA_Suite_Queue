package com.freddiemac.jms.util;

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * JndiHelper provides utility methods to obtain a JNDI Context for looking up
 * Connection Factories and JMS Queues. It uses standard WebLogic Server defaults,
 * but allows configuration overrides via JVM System properties.
 */
public class JndiHelper {

    private static final String DEFAULT_INITIAL_CONTEXT_FACTORY = "weblogic.jndi.WLInitialContextFactory";
    private static final String DEFAULT_PROVIDER_URL = "t3://localhost:7001";
    private static final String DEFAULT_CONNECTION_FACTORY_JNDI = "eis/wls/Queue";

    /**
     * Obtains the InitialContext based on system properties or defaults.
     *
     * @return InitialContext object
     * @throws NamingException if the JNDI context cannot be initialized
     */
    public static Context getInitialContext() throws NamingException {
        Hashtable<String, String> env = new Hashtable<>();
        
        // Load settings from system properties or fallback to WebLogic defaults
        String contextFactory = System.getProperty(Context.INITIAL_CONTEXT_FACTORY, DEFAULT_INITIAL_CONTEXT_FACTORY);
        String providerUrl = System.getProperty(Context.PROVIDER_URL, DEFAULT_PROVIDER_URL);
        String principal = System.getProperty(Context.SECURITY_PRINCIPAL);
        String credentials = System.getProperty(Context.SECURITY_CREDENTIALS);

        env.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
        env.put(Context.PROVIDER_URL, providerUrl);
        
        if (principal != null) {
            env.put(Context.SECURITY_PRINCIPAL, principal);
        }
        if (credentials != null) {
            env.put(Context.SECURITY_CREDENTIALS, credentials);
        }

        System.out.println("[JNDI] Creating InitialContext with:");
        System.out.println("  " + Context.INITIAL_CONTEXT_FACTORY + " = " + contextFactory);
        System.out.println("  " + Context.PROVIDER_URL + " = " + providerUrl);
        if (principal != null) {
            System.out.println("  " + Context.SECURITY_PRINCIPAL + " = " + principal);
        }
        
        return new InitialContext(env);
    }

    /**
     * Resolves the Connection Factory JNDI location, default is WebLogic Queue Connection Factory JNDI.
     *
     * @return the JNDI name of the Connection Factory
     */
    public static String getConnectionFactoryJndi() {
        return System.getProperty("jms.connection.factory", DEFAULT_CONNECTION_FACTORY_JNDI);
    }
}
