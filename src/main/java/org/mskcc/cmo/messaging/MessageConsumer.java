package org.mskcc.cmo.messaging;

import io.nats.client.Message;

public interface MessageConsumer {

    void onMessage(Message msg, Object message);

}
