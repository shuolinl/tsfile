package org.apache.tsfile.benchmark;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class TsFileBenchmark {
  private static final String SENSOR = "sensor";
  private static final String DEVICE = "device";
  private static final String TSFILE_PATH = "benchmark.tsfile";
  private static final Logger LOG = LoggerFactory.getLogger(TsFileBenchmark.class);
  private final List<List<IMeasurementSchema>> deviceList = new ArrayList<>();
  private static final DecimalFormat formater = new DecimalFormat("#,###.##");

  @Test
  public void testWrite() throws IOException {
    File f = FSFactoryProducer.getFSFactory().getFile(TSFILE_PATH);
    if (f.exists()) {
      Files.delete(f.toPath());
    }
    int deviceNum = 50;
    int sensorNum = 50;
    int pointsNum = 1000000;
    int tablet_size = 100000;
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      for (int device_id = 0; device_id < deviceNum; device_id++) {
        deviceList.add(new ArrayList<>());
        for (int sensor_id = 0; sensor_id < sensorNum; sensor_id++) {
          deviceList
              .get(device_id)
              .add(new MeasurementSchema(SENSOR + sensor_id, TSDataType.INT32));
        }
        tsFileWriter.registerTimeseries(new Path(DEVICE + device_id), deviceList.get(device_id));
      }

      long startTime = System.currentTimeMillis();
      int cur = 0;
      System.out.println("start");
      for (; cur < pointsNum; ) {
        if (cur + tablet_size > pointsNum) {
          tablet_size = pointsNum - cur;
        }

        for (int i = 0; i < deviceNum; i++) {
          String deviceId = DEVICE + i;
          Tablet tablet = new Tablet(deviceId, deviceList.get(i), tablet_size);
          tablet.initBitMaps();

          for (int row = 0; row < tablet_size; row++) {
            tablet.addTimestamp(row, 12345 + cur + row);
          }
          for (int j = 0; j < sensorNum; j++) {
            for (int row = 0; row < tablet_size; row++) {
              tablet.addValue(deviceList.get(i).get(j).getMeasurementName(), row, cur + row);
            }
          }
          tsFileWriter.writeTree(tablet);
          tablet.reset();
        }
        System.out.println("cur write:" + cur);
        cur += tablet_size;
      }
      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      System.out.println("write cost:" + (duration / 1000.0) + "s");
      long totalPoints = (long) sensorNum * deviceNum * pointsNum;
      System.out.println("total points : " + totalPoints + "points");
      System.out.println("write speed : " + totalPoints / (duration / 1000.0) + "points/s");
    } catch (WriteProcessException e) {
      throw new RuntimeException(e);
    }
    long fileSize = Files.size(f.toPath());
    System.out.println("file size:" + fileSize);
  }

  @Test
  public void testRead() throws IOException {
    TsFileSequenceReader fileReader = new TsFileSequenceReader(TSFILE_PATH);
    TsFileReader reader = new TsFileReader(fileReader);
    List<Path> pathList = new ArrayList<>();
    int num  = 0;
    for (int j = 0; j < 50; j++) {
      for (int i = 0; i < 50; i++) {
        pathList.add(new Path(DEVICE + j, SENSOR + i, true));
        num++;
      }
    }


    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    long startTime = System.currentTimeMillis();
    QueryDataSet dataSet = reader.query(queryExpression);
    long count = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      count++;
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("query all cost: " + (duration / 1000.0) + "s");
    System.out.println("query get points: " + formater.format(count * num) + "points");
    System.out.println("query speed : " +  formater.format(count * num /(duration / 1000.0)) + "points/s");
  }
}
