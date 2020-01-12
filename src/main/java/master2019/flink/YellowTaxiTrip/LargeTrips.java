package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * In this class the Large trips program has to be implemented
 * <p>
 * Large trips. It reports the vendors that do 5 or more trips during 3 hours that take at least 20
 * minutes. The output has the following format: VendorID, day, numberOfTrips,
 * tpep_pickup_datetime, tpep_dropoff_datetime, being tpep_dropoff_datetime the time the
 * last trip finishes and tpep_pickup_datetime the starting time of the first trip
 */
public class LargeTrips {
    static KeySelector<LargeTripsInputEvent, Integer> keySelector;
    static ReduceFunction<LargeTripsInputEvent> reduceFunction;
    static AscendingTimestampExtractor<LargeTripsInputEvent> timestampExtractor;

    public LargeTrips() {
        keySelector = new KeySelector<LargeTripsInputEvent, Integer>() {

            public Integer getKey(LargeTripsInputEvent value) throws Exception {
                return value.tuple3.f0; //Groups by Vendor Id
            }
        };

        reduceFunction = new ReduceFunction<LargeTripsInputEvent>() {
            public LargeTripsInputEvent reduce(LargeTripsInputEvent t0, LargeTripsInputEvent t1) throws Exception {
                if (!t0.isLong) {
                    return t1;
                } else if (!t1.isLong) {
                    return t0;
                } else {
                    Timestamp start = t0.tuple3.f1.getTime() < t1.tuple3.f1.getTime() ? t0.tuple3.f1 : t1.tuple3.f1;
                    Timestamp end = t0.tuple3.f2.getTime() > t1.tuple3.f2.getTime() ? t0.tuple3.f2 : t1.tuple3.f2;
                    LargeTripsInputEvent merge = new LargeTripsInputEvent(new Tuple3<Integer, Timestamp, Timestamp>(t0.tuple3.f0, start, end));
                    merge.count = t0.count + t1.count;
                    return merge;
                }
            }
        };

        timestampExtractor = new AscendingTimestampExtractor<LargeTripsInputEvent>() {
            @Override
            public long extractAscendingTimestamp(LargeTripsInputEvent t) {
                return t.tuple3.f1.getTime();
            }
        };

    }


    public static void run(SingleOutputStreamOperator<Event> mapStream) {

        SingleOutputStreamOperator<LargeTripsInputEvent> filteredStream = mapStream.map(new MapFunction<Event, LargeTripsInputEvent>() {
            public LargeTripsInputEvent map(Event in) {
                Tuple3<Integer, Timestamp, Timestamp> out = new Tuple3(
                        in.tuple18.f0,
                        in.tuple18.f1,
                        in.tuple18.f2
                );
                return new LargeTripsInputEvent(out);

            }

        });

        KeyedStream keyedStream = filteredStream.assignTimestampsAndWatermarks(timestampExtractor).keyBy(keySelector);

        SingleOutputStreamOperator<LargeTripsInputEvent> windowedStream =
                keyedStream.window(EventTimeSessionWindows.withGap(Time.hours(3))).reduce(LargeTrips.reduceFunction);

        SingleOutputStreamOperator<LargeTripsInputEvent> finalStream = windowedStream.filter(new FilterFunction<LargeTripsInputEvent>() {
            public boolean filter(LargeTripsInputEvent t) throws Exception {
                return t.count >= 5;
            }
        });

        finalStream.map(new MapFunction<LargeTripsInputEvent, LargeTripsOutputEvent>() {
            public LargeTripsOutputEvent map(LargeTripsInputEvent in) {
                return new LargeTripsOutputEvent(in);
            }
        }).writeAsCsv("output.csv");

    };

