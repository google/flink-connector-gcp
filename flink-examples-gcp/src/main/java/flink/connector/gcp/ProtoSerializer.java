package flink.connector.gcp;

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ProtoSerializer extends BigQueryProtoSerializer<GenericRecord> {

    @Override
    public ByteString serialize(GenericRecord message) throws BigQuerySerializationException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(byteArrayOutputStream);

        // Assuming the field order in your MyMessage.proto is: word (1), countStr (2)
        try {
            codedOutputStream.writeString(1, (String) message.get(0));
            codedOutputStream.writeString(2, (String) message.get(1));
            codedOutputStream.flush(); // Ensure everything is written
        } catch (IOException e) {
            throw new BigQuerySerializationException(e.getMessage());
        }

        // Convert ByteArrayOutputStream to ByteString
        return ByteString.copyFrom(byteArrayOutputStream.toByteArray());
    }
}
