package org.mskcc.cmo.messaging;

import io.nats.client.Message;
import org.mskcc.smile.commons.OpenTelemetryUtils.TraceMetadata;

public interface Gateway {

    void connect() throws Exception;

    void connect(String natsUrl) throws Exception;

    boolean isConnected();

    void publishWithTrace(String subject, Object message, TraceMetadata tmd) throws Exception;

    void publish(String subject, Object message) throws Exception;

    void publish(String msgId, String subject, Object message) throws Exception;

    void subscribe(String subject, Class messageClass,
            MessageConsumer messageConsumer) throws Exception;

    Message request(String subject, String message) throws Exception;

    void replySub(String subject, MessageConsumer consumer) throws Exception;

    void replyPublish(String subject, Object message) throws Exception;

    void shutdown() throws Exception;
}
