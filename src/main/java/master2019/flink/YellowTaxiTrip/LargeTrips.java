package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Date;
import java.sql.Timestamp;

/**
 * In this class the Large trips program has to be implemented
 * <p>
 * Large trips. It reports the vendors that do 5 or more trips during 3 hours that take at least 20
 * minutes. The output has the following format: VendorID, day, numberOfTrips,
 * tpep_pickup_datetime, tpep_dropoff_datetime, being tpep_dropoff_datetime the time the
 * last trip finishes and tpep_pickup_datetime the starting time of the first trip
 */
public class LargeTrips {
    static KeySelector<LargeTripsEvent, Integer> keySelector;
    static ReduceFunction<LargeTripsEvent> reduceFunction;
    static AscendingTimestampExtractor<LargeTripsEvent> timestampExtractor;
    public static Logger LOG = LoggerFactory.getLogger(LargeTripsEvent.class);

    public static void initializer() {
        //Used to group by vendorId
        keySelector = new KeySelector<LargeTripsEvent, Integer>() {
            public Integer getKey(LargeTripsEvent value) throws Exception {
                return value.getTuple3().f0;
            }
        };

        // the reduce function sums up the trip events longer than 20 minutes and stores them in the count variable
        reduceFunction = new ReduceFunction<LargeTripsEvent>() {
            public LargeTripsEvent reduce(LargeTripsEvent t0, LargeTripsEvent t1) throws Exception {
                if (!t0.isLong) {
                    return t1;
                } else if (!t1.isLong) {
                    return t0;
                } else {
                    Timestamp start = t0.getTuple3().f1.getTime() < t1.getTuple3().f1.getTime() ? t0.getTuple3().f1 : t1.getTuple3().f1;
                    Timestamp end = t0.getTuple3().f2.getTime() > t1.getTuple3().f2.getTime() ? t0.getTuple3().f2 : t1.getTuple3().f2;
                    LargeTripsEvent merge = new LargeTripsEvent(new Tuple3<Integer, Timestamp, Timestamp>(t0.getTuple3().f0, start, end));
                    merge.count = t0.count + t1.count;

                    /*LOG.info("T0.vendor {} and T1.vendor {}", t0.getTuple3().f0, t1.getTuple3().f0);
                    LOG.info("T0.count= {} and T1.count= {}", t0.count, t1.count);
                    LOG.info("merge.start= {} and merge.end= {}", start, end);*/
                    return merge;
                }
            }
        };

        // We define the event timestamp as the trip start to group the events in different windows
        timestampExtractor = new AscendingTimestampExtractor<LargeTripsEvent>() {
            @Override
            public long extractAscendingTimestamp(LargeTripsEvent t) {
                return t.getTuple3().f1.getTime();
            }
        };

    }


    public static SingleOutputStreamOperator<Tuple5<Integer, Date, Integer, Timestamp, Timestamp>> run(SingleOutputStreamOperator<Event> mapStream) {

        SingleOutputStreamOperator<LargeTripsEvent> filteredStream = mapStream.map(new MapFunction<Event, LargeTripsEvent>() {
            public LargeTripsEvent map(Event in) {
                Tuple3<Integer, Timestamp, Timestamp> out = new Tuple3(
                        in.tuple18.f0,
                        in.tuple18.f1,
                        in.tuple18.f2
                );
                return new LargeTripsEvent(out);

            }

        });

        KeyedStream keyedStream = filteredStream
                .assignTimestampsAndWatermarks(timestampExtractor)
                .keyBy(keySelector);

        SingleOutputStreamOperator<LargeTripsEvent> windowedStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.hours(3))).reduce(LargeTrips.reduceFunction);

        SingleOutputStreamOperator<LargeTripsEvent> min5stream = windowedStream.filter(new FilterFunction<LargeTripsEvent>() {
            public boolean filter(LargeTripsEvent t) throws Exception {
                return t.count >= 5;
            }
        });

        SingleOutputStreamOperator<Tuple5<Integer, Date, Integer, Timestamp, Timestamp>> outputStream = min5stream.map(new MapFunction<LargeTripsEvent, Tuple5<Integer, Date, Integer, Timestamp, Timestamp>>() {
            public Tuple5<Integer, Date, Integer, Timestamp, Timestamp> map(LargeTripsEvent t) {
                Tuple5<Integer, Date, Integer, Timestamp, Timestamp> tuple5 =
                        new Tuple5(
                                t.getTuple3().f0,                       //VendorID
                                new Date(t.getTuple3().f1.getTime()),   //day
                                t.count,                           //numberOfTrips
                                t.getTuple3().f1,                       //Timestamp first trip
                                t.getTuple3().f2);                      //Timestamp last trip

                return tuple5;
            }
        });

        return outputStream;

    }
}

class LargeTripsEvent {

    Logger LOG = LoggerFactory.getLogger(LargeTripsEvent.class);


    private Tuple3<Integer, Timestamp, Timestamp> tuple3; //VendorID, tpep_pickup_datetime, tpep_dropoff_datetime
    boolean isLong;                               // determines if trip is longer than 20 minutes
    int count;                                    // used to aggregate trips in the reduce step.

    public LargeTripsEvent(Tuple3<Integer, Timestamp, Timestamp> t) {
        tuple3 = t;
        //LOG.info(String.valueOf(tripDuration()));
        isLong = tripDuration() >= 20;
        count = isLong ? 1 : 0;  // sets value 1 if the trip is long
    }

    public int tripDuration() {
        long milliseconds = this.tuple3.f2.getTime() - this.tuple3.f1.getTime();
        int minutes = ((int) (milliseconds / 1000) % 3600) / 60;
        return minutes;
    }

    public Tuple3<Integer, Timestamp, Timestamp> getTuple3() {
        return tuple3;
    }
}
