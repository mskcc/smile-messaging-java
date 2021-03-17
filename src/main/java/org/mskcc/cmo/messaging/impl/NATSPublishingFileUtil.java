package org.mskcc.cmo.messaging.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.mskcc.cmo.messaging.FileUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class NATSPublishingFileUtil implements FileUtil {
    @Value("${metadb.publishing_failures_filepath}")
    private String publishingFailuresFilepath;

    private static final String PUB_FAILURES_FILE_HEADER = "DATE\tTOPIC\tMESSAGE\n";

    @Override
    public void savePublishFailureMessage(String topic, String message) throws IOException {
        BufferedWriter fileWriter = getOrCreateFileWriterWithHeader(publishingFailuresFilepath,
                PUB_FAILURES_FILE_HEADER);
        fileWriter.write(generatePublishFailureRecord(topic, message));
        fileWriter.close();
    }

    @Override
    public void writeToFile(String filepath, String header, String value)
            throws IOException {
        BufferedWriter fileWriter = getOrCreateFileWriterWithHeader(filepath, header);
        fileWriter.write(value);
        fileWriter.close();
    }

    private BufferedWriter getOrCreateFileWriterWithHeader(String filepath, String header)
            throws IOException {
        File f = new File(filepath);
        Boolean fileCreated = Boolean.FALSE;
        if (!f.exists()) {
            f.createNewFile();
            fileCreated = Boolean.TRUE;
        }
        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(f, true));
        if (fileCreated) {
            fileWriter.write(header);
        }
        return fileWriter;
    }

    /**
     * Generates record to write to publishing failure file.
     * @param topic
     * @param message
     * @return String
     */
    private String generatePublishFailureRecord(String topic, String message) {
        String currentDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        StringBuilder builder = new StringBuilder();
        builder.append(currentDate)
                .append("\t")
                .append(topic)
                .append("\t")
                .append(message)
                .append("\n");
        return builder.toString();
    }
}
