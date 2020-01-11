package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.sql.Timestamp;

/**
 * In this class the Large trips program has to be implemented
 *
 * Large trips. It reports the vendors that do 5 or more trips during 3 hours that take at least 20
 * minutes. The output has the following format: VendorID, day, numberOfTrips,
 * tpep_pickup_datetime, tpep_dropoff_datetime, being tpep_dropoff_datetime the time the
 * last trip finishes and tpep_pickup_datetime the starting time of the first trip
 */
public class LargeTrips {
    public static void run(SingleOutputStreamOperator<Event> mapStream){

        mapStream.filter(new FilterFunction<Event>() {
            public boolean filter(Event in) throws Exception {


                return in.tuple18.f1.equals("sensor1");
            }
        });


    }
}
