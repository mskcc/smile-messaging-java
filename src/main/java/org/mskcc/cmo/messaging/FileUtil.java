package org.mskcc.cmo.messaging;

import java.io.IOException;

public interface FileUtil {
    void savePublishFailureMessage(String topic, String message) throws IOException;
    void writeToFile(String filepath, String header, String value) throws IOException;
}
