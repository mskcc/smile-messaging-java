package org.mskcc.cmo.messaging;

import io.nats.client.Message;

public interface Gateway {

    void connect() throws Exception;

    void connect(String natsUrl) throws Exception;

    boolean isConnected();

    void publish(String subject, Object message) throws Exception;
    
    void publish(String msgId, String subject, Object message) throws Exception;

    void subscribe(String subject, Class messageClass,
            MessageConsumer messageConsumer) throws Exception;
    
    Message request(String subject, String message) throws Exception;
    
    void replySub(String subject, MessageConsumer consumer) throws Exception;
    
    void replyPublish(String subject, Object message) throws Exception;

    void shutdown() throws Exception;
}
