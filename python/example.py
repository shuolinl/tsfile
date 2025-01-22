from tsfile import TsFileWriter, TsFileReader, TSDataType, TSEncoding, Compressor
from tsfile import DeviceSchema, TimeseriesSchema, RowRecord, Field
from tsfile import ResultSet
import os


data_dir = os.path.join(os.path.dirname(__file__), "test.tsfile")
DEVICE_NAME = "root.device"
if os.path.exists(data_dir):
    os.remove(data_dir)
writer = TsFileWriter(data_dir)
timeseries = TimeseriesSchema("temp1", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED)
timeseries2 = TimeseriesSchema("temp2", TSDataType.INT64, TSEncoding.PLAIN, Compressor.UNCOMPRESSED)
device = DeviceSchema(DEVICE_NAME, [timeseries, timeseries2])
writer.register_device(device)

rc = RowRecord(DEVICE_NAME, 100, [Field("temp1", TSDataType.INT32, 10), Field("temp2", TSDataType.INT64, 10)])
writer.write_row_record(rc)
writer.close()

reader = TsFileReader(data_dir)
paths = ["temp1", "temp2"]
result = reader.query_timeseries("root.device", paths, 0, 110)
while result.has_next():
    print(result.get_value_by_index(0))
    print(result.get_value_by_index(1))
    print(result.get_value_by_name("temp1"))
    print(result.get_value_by_name("temp2"))



