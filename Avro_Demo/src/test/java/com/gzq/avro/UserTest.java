package com.gzq.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-05 16:11.
 */
public class UserTest {


    @Test
    public void testReadWithoutUser() throws IOException {
        File file = new File("users2.avro");
        Schema schema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user =null;
        //for (GenericRecord genericRecord : dataFileReader) {
        //    System.out.println( genericRecord);
        //}
        System.out.println("--------------------");
        while(dataFileReader.hasNext()){
            user = dataFileReader.next();
            System.out.println("user = " + user);
        }

    }

    /**
     * Serializing
     *
     * @throws java.io.IOException
     */
    @Test
    public void testWriteWithoutUser() throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));

        GenericData.Record user1 = new GenericData.Record(schema);
        user1.put("name","Tom");
        user1.put("favorite_number", 256);

        GenericData.Record user2 = new GenericData.Record(schema);
        user2.put("name", "Jreey");
        user2.put("favorite_number", 128);

        File file = new File("users2.avro");
        SpecificDatumWriter<GenericData.Record> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericData.Record> dataDataFileWriter = new DataFileWriter<>(datumWriter);
        dataDataFileWriter.create(schema,file);
        dataDataFileWriter.append(user1);
        dataDataFileWriter.append(user2);
        dataDataFileWriter.close();

    }


    /**
     * Serializing
     * @throws java.io.IOException
     */
    @Test
    public void testWriteWithUser() throws IOException {
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);
        // Leave favorite color null
        // Alternate constructor
        User user2 = new User("Ben", 7, "red");
        // Construct via builder
        User user3 = User.newBuilder().setName("Charlie").setFavoriteColor("blue").setFavoriteNumber(null).build();


        // Serialize user1, user2 and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        File file = new File("users.avro");

        dataFileWriter.create(user1.getSchema(), file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }

    /**
     * Deserializing
     * @throws java.io.IOException
     */
    @Test
    public void testReadWithUser() throws IOException {
        // Deserialize Users from disk
        File file = new File("users.avro");
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }


}
