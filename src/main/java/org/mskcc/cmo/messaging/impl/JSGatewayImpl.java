package org.mskcc.cmo.messaging.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamOptions;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NUID;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.Options.Builder;
import io.nats.client.PublishOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.messaging.utils.SSLUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class JSGatewayImpl implements Gateway {
    private final Log LOG = LogFactory.getLog(JSGatewayImpl.class);
    private final Integer RECONNECTION_ATTEMPTS = 10;
    private final Integer RECONNECTION_TIME_SEC = 30;
    private final Integer REQREPLY_ATTEMPTS = 10;
    private final CountDownLatch publishingShutdownLatch = new CountDownLatch(1);
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, JetStreamSubscription> subscribers = new HashMap<>();
    private BlockingQueue<PublishingQueueTask> publishingQueue = new LinkedBlockingQueue<>();
    private ExecutorService exec = Executors.newSingleThreadExecutor();
    private volatile Boolean shutdownInitiated = Boolean.FALSE;

    // connection variables
    @Value("${nats.url}")
    public String natsUrl;

    @Value("${nats.consumer_name}")
    public String consumerName;

    @Value("${nats.consumer_password}")
    public String consumerPassword;

    @Value("${nats.filter_subject}")
    public String filterSubject;

    @Value("${nats.request_wait_time_in_seconds:10}")
    public int requestWaitTime;

    private Connection natsConnection;
    private JetStream jsConnection;

    @Value("${nats.tls_channel:false}")
    private boolean tlsChannel;

    @Autowired
    SSLUtils sslUtils;

    @Override
    public void connect() throws Exception {
        connect(natsUrl);
    }

    @Override
    public void connect(String natsUrl) throws Exception {
        Builder builder = new Builder()
                .server(natsUrl)
                .oldRequestStyle()
                .userInfo(consumerName, consumerPassword);
        if (tlsChannel) {
            builder.sslContext(sslUtils.createSSLContext());
        }
        this.natsConnection = Nats.connect(builder.build());
        this.jsConnection = natsConnection.jetStream();
        exec.execute(new NATSPublisher(natsConnection.getOptions()));
    }

    @Override
    public  boolean isConnected() {
        if (natsConnection == null) {
            return Boolean.FALSE;
        }
        return (natsConnection != null && natsConnection.getStatus() != null
                && (natsConnection.getStatus().equals(Connection.Status.CONNECTED)));
    }

    private void reconnect() throws Exception {
        LOG.warn("Gateway connection has not been established or was lost - attemping to reconnect");
        Integer currentAttempt = 1;
        while (currentAttempt <= RECONNECTION_ATTEMPTS
                && !isConnected()) {
            LOG.info("Attempt #" + currentAttempt + "/" + RECONNECTION_ATTEMPTS
                    + " to reestablish connection to NATS server...");
            TimeUnit.SECONDS.sleep(RECONNECTION_TIME_SEC);
            connect();
        }
        // if all attempts failed then throw illegal state exception
        if (!isConnected()) {
            throw new IllegalStateException("Failed to reestablish connection to NATS server");
        } else {
            LOG.info("Successfully reconnected to NATS server!");
        }
    }

    @Override
    public void publishWithTracePropagation(String subject, Object message, Map<String, String> traceMetadata)
        throws Exception {
        if (!isConnected()) {
            reconnect();
        }
        if (!shutdownInitiated) {
            publishingQueue.put(new PublishingQueueTask(subject, message, traceMetadata));
        } else {
            LOG.error("Shutdown initiated, not accepting publish request: \n" + message);
            throw new IllegalStateException("Shutdown initiated, not accepting anymore publish requests");
        }
    }

    @Override
    public void publish(String subject, Object message) throws Exception {
        if (!isConnected()) {
            reconnect();
        }
        if (!shutdownInitiated) {
            publishingQueue.put(new PublishingQueueTask(subject, message));
        } else {
            LOG.error("Shutdown initiated, not accepting publish request: \n" + message);
            throw new IllegalStateException("Shutdown initiated, not accepting anymore publish requests");
        }
    }

    @Override
    public void publish(String msgId, String subject, Object message) throws Exception {
        if (!isConnected()) {
            reconnect();
        }
        if (!shutdownInitiated) {
            publishingQueue.put(new PublishingQueueTask(msgId, subject, message));
        } else {
            LOG.error("Shutdown initiated, not accepting publish request: \n" + message);
            throw new IllegalStateException("Shutdown initiated, not accepting anymore publish requests");
        }
    }

    @Override
    public void subscribe(String subject, Class messageClass,
            MessageConsumer messageConsumer) throws Exception {
        if (!isConnected()) {
            reconnect();
        }
        if (!subscribers.containsKey(subject)) {
            Dispatcher dispatcher = natsConnection.createDispatcher();
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable(consumerName)
                    .build();
            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .configuration(consumerConfig)
                    .build();
            JetStreamSubscription sub = jsConnection.subscribe(filterSubject, dispatcher,
                msg -> onMessage(subject, msg, messageClass, messageConsumer), false, options);
            subscribers.put(subject, sub);
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Gateway connection has not been established");
        }
        exec.shutdownNow();
        shutdownInitiated = Boolean.TRUE;
        publishingShutdownLatch.await();
        natsConnection.close();
    }

    /**
     * Configure basic message handler logic.
     * @param subject
     * @param msg
     * @param messageClass
     * @param messageConsumer
     */
    public void onMessage(String subject, Message msg, Class messageClass, MessageConsumer messageConsumer) {
        Boolean subjectMatches = Boolean.FALSE;
        if (msg.hasHeaders()) {
            List<String> hdrContents = msg.getHeaders().get("Nats-Msg-Subject");
            if (hdrContents.size() == 1 && hdrContents.get(0).endsWith(subject)) {
                subjectMatches = Boolean.TRUE;
            }
        }

        String payload = new String(msg.getData(), StandardCharsets.UTF_8);
        Object message = mapper.convertValue(payload, messageClass);
        if (message != null && subjectMatches) {
            msg.ack();
            messageConsumer.onMessage(msg, message);
        }
    }

    private class NATSPublisher implements Runnable {
        Connection natsConn;
        JetStream jsConn;
        boolean interrupted;

        /**
         * NATSPublisher constructor.
         * @param options
         * @throws IOException
         * @throws InterruptedException
         */
        public NATSPublisher(Options options) throws IOException, InterruptedException {
            this.natsConn = Nats.connect(options);
            this.jsConn = natsConn.jetStream(JetStreamOptions.DEFAULT_JS_OPTIONS);
            this.interrupted = Boolean.FALSE;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    PublishingQueueTask task = publishingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (task != null && task.payload != null) {
                        try {
                            PublishAck ack = jsConn.publish(task.getMessage(),
                                    PublishOptions.builder().messageId(task.msgId).build());
                            if (ack.getError() != null) {
                                LOG.error(ack.getError());
                            }
                        } catch (Exception e) {
                            interrupted = Boolean.TRUE;
                            if (e instanceof IOException
                                    && e.getLocalizedMessage().contains("InterruptedException")) {
                                LOG.info("Interruption caught, exiting...");
                            } else {
                                LOG.error("Error during attempt to publish on topic: " + task.subject, e);
                            }
                        }
                    }
                } catch (InterruptedException ex) {
                    interrupted = Boolean.TRUE;
                }
                if ((interrupted || shutdownInitiated) && publishingQueue.isEmpty()) {
                    break;
                }
            }
            try {
                // close connection
                natsConn.close();
            } catch (InterruptedException ex) {
                LOG.error("Error during attempt to close NATS connection", ex);
            }
            publishingShutdownLatch.countDown();
        }
    }

    private class PublishingQueueTask {
        String msgId;
        String subject;
        Object payload;
        Map<String, String> traceMetadata;
        boolean isBinary;

        public PublishingQueueTask(String subject, Object payload, Map<String, String> traceMetadata) {
            this.msgId = NUID.nextGlobal();
            this.subject = subject;
            this.payload = payload;
            this.traceMetadata = traceMetadata;
            this.isBinary = payload instanceof byte[];
        }

        public PublishingQueueTask(String subject, Object payload) {
            this.msgId = NUID.nextGlobal();
            this.subject = subject;
            this.payload = payload;
            this.isBinary = payload instanceof byte[];
        }

        public PublishingQueueTask(String msgId, String subject, Object payload) {
            this.msgId = msgId + "_" + subject;
            this.subject = subject;
            this.payload = payload;
            this.isBinary = payload instanceof byte[];
        }

        public Message getMessage() throws JsonProcessingException {
            Headers headers = new Headers().add("Nats-Msg-Subject", subject);
            if (traceMetadata != null) {
                for (Map.Entry<String, String> entry : traceMetadata.entrySet()) {
                    headers.add(entry.getKey(), entry.getValue());
                }
            }
            byte[] data = isBinary ? (byte[]) payload : getPayloadAsString().getBytes();
            return NatsMessage.builder()
                    .subject(subject)
                    .data(data)
                    .headers(headers)
                    .build();
        }

        public String getPayloadAsString() throws JsonProcessingException {
            if (isBinary) {
                return Base64.getEncoder().encodeToString((byte[]) payload);
            }
            return mapper.writeValueAsString(payload);
        }
    }

    @Override
    public Message request(String subject, String message)
            throws Exception {
        if (!isConnected()) {
            reconnect();
        }
        if (!shutdownInitiated) {
            Integer currentAttempt = 1;
            while (currentAttempt <= REQREPLY_ATTEMPTS) {
                try {
                    Future<Message> replyFuture = natsConnection.request(NatsMessage.builder()
                            .subject(subject)
                            .data(message, StandardCharsets.UTF_8)
                            .build());
                    Message reply = replyFuture.get(requestWaitTime, TimeUnit.SECONDS);
                    return reply;
                } catch (TimeoutException e) {
                    currentAttempt++;
                }
            }
            // throw timeout exception if exceeded num attempts allowed for req-reply
            throw new TimeoutException("Exceeded allowed number of attempts for request-reply");
        } else {
            LOG.error("Shutdown initiated, not accepting publish request: \n" + message);
            throw new IllegalStateException("Shutdown initiated, not accepting anymore requests");
        }
    }

    @Override
    public void replySub(String subject, MessageConsumer consumer) throws Exception {
        if (!isConnected()) {
            reconnect();
        }
        if (!shutdownInitiated) {
            Dispatcher d = natsConnection.createDispatcher(new MessageHandler() {
                    @Override
                    public void onMessage(Message msg) throws InterruptedException {
                        consumer.onMessage(msg, String.class);
                    }
                });
            d.subscribe(subject);
        } else {
            LOG.error("Shutdown initiated, not handling replySub on topic: " + subject);
            throw new IllegalStateException("Shutdown initiated, not accepting anymore replySub messages");
        }
    }

    @Override
    public void replyPublish(String subject, Object data) throws Exception {
        if (!isConnected()) {
            reconnect();
        }
        if (!shutdownInitiated) {
            natsConnection.publish(subject, mapper.convertValue(data, String.class).getBytes());
        } else {
            LOG.error("Shutdown initiated, not handling replyPublish on topic: " + subject);
            throw new IllegalStateException("Shutdown initiated, not accepting anymore publish requests");
        }
    }
}
