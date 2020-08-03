/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.parquet;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test on the {@link ParquetIO}. */
@RunWith(JUnit4.class)
public class ParquetIOTest implements Serializable {

  @Rule public transient TestPipeline mainPipeline = TestPipeline.create();

  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String SCHEMA_STRING =
      "{"
          + "\"type\":\"record\", "
          + "\"name\":\"testrecord\","
          + "\"fields\":["
          + "    {\"name\":\"name\",\"type\":\"string\"},"
              + "    {\"name\":\"id\",\"type\":\"string\"}"
          + "  ]"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  private static final String[] SCIENTISTS =
      new String[] {
        "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie",
        "Faraday", "Newton", "Bohr", "Galilei", "Maxwell"
      };

  @Test
  public void testChange(){
    String schema_string="message schema {\n" +
            "  optional binary the_geom (UTF8);\n" +
            "  optional binary zip_code (UTF8);\n" +
            "  optional binary anon_ticket_number (UTF8);\n" +
            "  optional binary issue_datetime (UTF8);\n" +
            "  optional binary state (UTF8);\n" +
            "  optional binary anon_plate_id (UTF8);\n" +
            "  optional binary division (UTF8);\n" +
            "  optional binary the_geom_webmercator (UTF8);\n" +
            "  optional binary violation_desc (UTF8);\n" +
            "  optional binary fine (UTF8);\n" +
            "  optional binary issuing_agency (UTF8);\n" +
            "  optional binary lat (UTF8);\n" +
            "  optional binary lon (UTF8);\n" +
            "  optional binary gps (UTF8);\n" +
            "  optional binary location (UTF8);\n" +
            "}";

    MessageType parquetSchema=MessageTypeParser.parseMessageType(schema_string);
    Schema schema=new AvroSchemaConverter().convert(parquetSchema);
    PCollection<GenericRecord> readBack =
            readPipeline.apply(
                    ParquetIO.read(schema).withSplit().from("/Users/xiajiadai/Downloads/park.parquet"));
    readPipeline.run().waitUntilFinish();
  }
  @Test
  public void testWithLocalFile(){
    String parquet_Schema_String="message spark_schema {\n" +
            "  optional binary id (UTF8);\n" +
            "  optional double log_duration;\n" +
            "  optional double pickup_longitude;\n" +
            "  optional double pickup_latitude;\n" +
            "  optional double dropoff_longitude;\n" +
            "  optional double dropoff_latitude;\n" +
            "  optional double great_circle_distance;\n" +
            "  optional double distance;\n" +
            "  optional int64 snow;\n" +
            "  optional int64 holiday;\n" +
            "  optional int64 vendor_id;\n" +
            "  optional int64 pickup_hour_0;\n" +
            "  optional int64 pickup_hour_1;\n" +
            "  optional int64 pickup_hour_2;\n" +
            "  optional int64 pickup_hour_3;\n" +
            "  optional int64 pickup_hour_4;\n" +
            "  optional int64 pickup_hour_5;\n" +
            "  optional int64 pickup_hour_6;\n" +
            "  optional int64 pickup_hour_7;\n" +
            "  optional int64 pickup_hour_8;\n" +
            "  optional int64 pickup_hour_9;\n" +
            "  optional int64 pickup_hour_10;\n" +
            "  optional int64 pickup_hour_11;\n" +
            "  optional int64 pickup_hour_12;\n" +
            "  optional int64 pickup_hour_13;\n" +
            "  optional int64 pickup_hour_14;\n" +
            "  optional int64 pickup_hour_15;\n" +
            "  optional int64 pickup_hour_16;\n" +
            "  optional int64 pickup_hour_17;\n" +
            "  optional int64 pickup_hour_18;\n" +
            "  optional int64 pickup_hour_19;\n" +
            "  optional int64 pickup_hour_20;\n" +
            "  optional int64 pickup_hour_21;\n" +
            "  optional int64 pickup_hour_22;\n" +
            "  optional int64 pickup_hour_23;\n" +
            "  optional int64 pickup_weekday_0;\n" +
            "  optional int64 pickup_weekday_1;\n" +
            "  optional int64 pickup_weekday_2;\n" +
            "  optional int64 pickup_weekday_3;\n" +
            "  optional int64 pickup_weekday_4;\n" +
            "  optional int64 pickup_weekday_5;\n" +
            "  optional int64 pickup_weekday_6;\n" +
            "  optional int64 pickup_month_1;\n" +
            "  optional int64 pickup_month_2;\n" +
            "  optional int64 pickup_month_3;\n" +
            "  optional int64 pickup_month_4;\n" +
            "  optional int64 pickup_month_5;\n" +
            "  optional int64 pickup_month_6;\n" +
            "  optional int64 passenger_count_0;\n" +
            "  optional int64 passenger_count_1;\n" +
            "  optional int64 passenger_count_2;\n" +
            "  optional int64 passenger_count_3;\n" +
            "  optional int64 passenger_count_4;\n" +
            "  optional int64 passenger_count_5;\n" +
            "  optional int64 passenger_count_6;\n" +
            "  optional int64 passenger_count_7;\n" +
            "  optional int64 C0;\n" +
            "  optional int64 C1;\n" +
            "  optional int64 C2;\n" +
            "  optional int64 C3;\n" +
            "  optional int64 C4;\n" +
            "  optional int64 C5;\n" +
            "  optional int64 C6;\n" +
            "  optional int64 C7;\n" +
            "  optional int64 C8;\n" +
            "  optional int64 C9;\n" +
            "  optional int64 C10;\n" +
            "  optional int64 C11;\n" +
            "  optional int64 C12;\n" +
            "  optional int64 C13;\n" +
            "  optional int64 C14;\n" +
            "  optional int64 C15;\n" +
            "  optional int64 C16;\n" +
            "  optional int64 C17;\n" +
            "  optional int64 C18;\n" +
            "  optional int64 C19;\n" +
            "  optional int64 C20;\n" +
            "  optional int64 C21;\n" +
            "  optional int64 C22;\n" +
            "  optional int64 C23;\n" +
            "  optional int64 C24;\n" +
            "  optional int64 C25;\n" +
            "  optional int64 C26;\n" +
            "  optional int64 C27;\n" +
            "  optional int64 C28;\n" +
            "  optional int64 C29;\n" +
            "  optional int64 C30;\n" +
            "  optional int64 C31;\n" +
            "  optional int64 C32;\n" +
            "  optional int64 C33;\n" +
            "  optional int64 C34;\n" +
            "  optional int64 C35;\n" +
            "  optional int64 C36;\n" +
            "  optional int64 C37;\n" +
            "  optional int64 C38;\n" +
            "  optional int64 C39;\n" +
            "  optional int64 C40;\n" +
            "  optional int64 C41;\n" +
            "  optional int64 C42;\n" +
            "  optional int64 C43;\n" +
            "  optional int64 C44;\n" +
            "  optional int64 C45;\n" +
            "  optional int64 C46;\n" +
            "  optional int64 C47;\n" +
            "  optional int64 C48;\n" +
            "  optional int64 C49;\n" +
            "  optional int64 C50;\n" +
            "  optional int64 C51;\n" +
            "  optional int64 C52;\n" +
            "  optional int64 C53;\n" +
            "  optional int64 C54;\n" +
            "  optional int64 C55;\n" +
            "  optional int64 C56;\n" +
            "  optional int64 C57;\n" +
            "  optional int64 C58;\n" +
            "  optional int64 C59;\n" +
            "  optional int64 C60;\n" +
            "  optional int64 C61;\n" +
            "  optional int64 C62;\n" +
            "  optional int64 C63;\n" +
            "  optional int64 C64;\n" +
            "  optional int64 C65;\n" +
            "  optional int64 C66;\n" +
            "  optional int64 C67;\n" +
            "  optional int64 C68;\n" +
            "  optional int64 C69;\n" +
            "  optional int64 C70;\n" +
            "  optional int64 C71;\n" +
            "  optional int64 C72;\n" +
            "  optional int64 C73;\n" +
            "  optional int64 C74;\n" +
            "  optional int64 C75;\n" +
            "  optional int64 C76;\n" +
            "  optional int64 C77;\n" +
            "  optional int64 C78;\n" +
            "  optional int64 C79;\n" +
            "  optional int64 C80;\n" +
            "  optional int64 C81;\n" +
            "  optional int64 C82;\n" +
            "  optional int64 C83;\n" +
            "  optional int64 C84;\n" +
            "  optional int64 C85;\n" +
            "  optional int64 C86;\n" +
            "  optional int64 C87;\n" +
            "  optional int64 C88;\n" +
            "  optional int64 C89;\n" +
            "  optional int64 C90;\n" +
            "  optional int64 C91;\n" +
            "  optional int64 C92;\n" +
            "  optional int64 C93;\n" +
            "  optional int64 C94;\n" +
            "  optional int64 C95;\n" +
            "  optional int64 C96;\n" +
            "  optional int64 C97;\n" +
            "  optional int64 C98;\n" +
            "  optional int64 C99;\n" +
            "}";
    MessageType parquetSchema=MessageTypeParser.parseMessageType(parquet_Schema_String);
    Schema schema=new AvroSchemaConverter().convert(parquetSchema);
    PCollection<GenericRecord> readBack =
            readPipeline.apply(
                    ParquetIO.read(schema).from("/Users/xiajiadai/Downloads/taxi.parquet"));
    readPipeline.run().waitUntilFinish();

  }
  @Test
  public void testWriteAndRead() {
    List<GenericRecord> records = generateGenericRecords(1000);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(SCHEMA).from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));
    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadWithSplit() {
    List<GenericRecord> records = generateGenericRecords(1000);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBackWithSplit =
        readPipeline.apply(
            ParquetIO.read(SCHEMA)
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                .withSplit());
    PAssert.that(readBackWithSplit).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadFiles() {
    List<GenericRecord> records = generateGenericRecords(1000);

    PCollection<GenericRecord> writeThenRead =
        mainPipeline
            .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                FileIO.<GenericRecord>write()
                    .via(ParquetIO.sink(SCHEMA))
                    .to(temporaryFolder.getRoot().getAbsolutePath()))
            .getPerDestinationOutputFilenames()
            .apply(Values.create())
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ParquetIO.readFiles(SCHEMA));

    PAssert.that(writeThenRead).containsInAnyOrder(records);

    mainPipeline.run().waitUntilFinish();
  }

  private List<GenericRecord> generateGenericRecords(long count) {
    ArrayList<GenericRecord> data = new ArrayList<>();
    GenericRecordBuilder builder = new GenericRecordBuilder(SCHEMA);
    for (int i = 0; i < count; i++) {
      int index = i % SCIENTISTS.length;
      GenericRecord record = builder.set("name", SCIENTISTS[index]).set("id",Integer.toString(i)).build();
      data.add(record);
    }
    return data;
  }

  @Test
  public void testReadDisplayData() {
    DisplayData displayData = DisplayData.from(ParquetIO.read(SCHEMA).from("foo.parquet"));

    Assert.assertThat(displayData, hasDisplayItem("filePattern", "foo.parquet"));
  }

  public static class TestRecord {
    String name;

    public TestRecord(String name) {
      this.name = name;
    }
  }

  @Test(expected = org.apache.beam.sdk.Pipeline.PipelineExecutionException.class)
  public void testWriteAndReadUsingReflectDataSchemaWithoutDataModelThrowsException() {
    Schema testRecordSchema = ReflectData.get().getSchema(TestRecord.class);

    List<GenericRecord> records = generateGenericRecords(1000);
    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(testRecordSchema)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(testRecordSchema))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(testRecordSchema)
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadUsingReflectDataSchemaWithDataModel() {
    Schema testRecordSchema = ReflectData.get().getSchema(TestRecord.class);

    List<GenericRecord> records = generateGenericRecords(1000);
    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(testRecordSchema)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(testRecordSchema))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(testRecordSchema)
                .withAvroDataModel(GenericData.get())
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }
}
