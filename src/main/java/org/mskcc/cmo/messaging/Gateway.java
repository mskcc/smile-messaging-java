package org.mskcc.cmo.messaging;

public interface Gateway {

    void connect() throws Exception;

    void connect(String natsUrl) throws Exception;

    boolean isConnected();

    void publish(String subject, Object message) throws Exception;
    
    void publish(String msgId, String subject, Object message) throws Exception;

    void subscribe(String subject, Class messageClass,
            MessageConsumer messageConsumer) throws Exception;
    
    String request(String subject, Object message);
    
    void reply(String subject, Object message);

    void shutdown() throws Exception;
}
