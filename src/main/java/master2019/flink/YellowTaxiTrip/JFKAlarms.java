package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import java.sql.Timestamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Collector;
import java.util.Iterator;



/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {
    
    public static SingleOutputStreamOperator airportTrips(SingleOutputStreamOperator input){

        //First we keep only the variables that we need for this task
        SingleOutputStreamOperator<Tuple5<Integer,Timestamp,Timestamp,Integer,Integer>> mapStream = input.
            map(new MapFunction<Tuple18<Integer,Timestamp,Timestamp,Integer,Float,Integer,String,Integer,Integer,Integer,Float,Float,Float,Float,Float,Float,Float,Float>,Tuple5<Integer,Timestamp,Timestamp,Integer,Integer>>() {
                public Tuple5<Integer,Timestamp,Timestamp,Integer,Integer> map(Tuple18<Integer, Timestamp,Timestamp,Integer,Float,Integer,String,Integer,Integer,Integer,Float,Float,Float,Float,Float,Float,Float,Float> in){
                    Tuple5<Integer,Timestamp,Timestamp,Integer,Integer> out = new Tuple5(
                        in.getField(0),
                        in.getField(1),
                        in.getField(2),
                        in.getField(3),
                        in.getField(5)
                    );
                    return out;
                }
            });
        
        SingleOutputStreamOperator<Tuple5<Integer,Timestamp,Timestamp,Integer,Integer>> filterStream = mapStream.
            filter(new FilterFunction<Tuple5<Integer,Timestamp,Timestamp,Integer,Integer>>() {
                public boolean filter(Tuple5<Integer,Timestamp,Timestamp,Integer,Integer> in) throws Exception{
                    Integer numPassenger = in.f3;
                    if(numPassenger >= 2){
                        return in.f4.equals(2);
                    } 
                    else{
                        return false;
                    }          
                }
        });

        SingleOutputStreamOperator<Tuple4<Integer,Timestamp,Timestamp,Integer>> mapStream1 = filterStream.
            map(new MapFunction<Tuple5<Integer,Timestamp,Timestamp,Integer,Integer>,Tuple4<Integer,Timestamp,Timestamp,Integer>>() {
                public Tuple4<Integer,Timestamp,Timestamp,Integer> map(Tuple5<Integer,Timestamp,Timestamp,Integer,Integer> in){
                    Tuple4<Integer,Timestamp,Timestamp,Integer> out = new Tuple4(
                        in.getField(0),
                        in.getField(1),
                        in.getField(2),
                        in.getField(3)
                    );
                    return out;
                }
        });

        AscendingTimestampExtractor tsExtractor = new AscendingTimestampExtractor<Tuple4<Integer,Timestamp,Timestamp,Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Integer,Timestamp,Timestamp,Integer> in) {
                return in.f1.getTime();
            }
        };

        KeyedStream<Tuple4<Integer,Timestamp,Timestamp,Integer>,Tuple> keyedStream = mapStream1.
        assignTimestampsAndWatermarks(tsExtractor).
        keyBy(1);

        SingleOutputStreamOperator<Tuple4<Integer,Timestamp,Timestamp,Integer>> windowStream = keyedStream.
            window(TumblingEventTimeWindows.of(Time.hours(1))).apply(new PassengerSum());



        /**KeyedStream<Tuple4<Integer,Long,Long,Integer>,Tuple> keyedStream = mapStream1.keyBy(0);
        SingleOutputStreamOperator<Tuple4<Integer,Long,Long,Integer>> tumblingWindow = keyedStream.
            window(TumblingProcessingTimeWindows.of(Time.milliseconds(3000))).apply(new PassengerSum());

        
        SingleOutputStreamOperator<Tuple4<Integer,Timestamp,Timestamp,Integer>> mapStream2 = tumblingWindow.
                map(new MapFunction<Tuple4<Integer,Long,Long,Integer>,Tuple4<Integer,Timestamp,Timestamp,Integer>>() {
                    public Tuple4<Integer,Timestamp,Timestamp,Integer> map(Tuple4<Integer,Long,Long,Integer> in){
                        Tuple4<Integer,Timestamp,Timestamp,Integer> out = new Tuple4(
                            in.getField(0),
                            new Timestamp((Long)in.getField(1)),
                            new Timestamp((Long)in.getField(2)),
                            in.getField(3)
                        );
                    return out;
                }
            });**/

        return windowStream; 
    }



    public static class PassengerSum implements WindowFunction<Tuple4<Integer,Timestamp,Timestamp,Integer>, Tuple4<Integer,Timestamp,Timestamp,Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Integer,Timestamp,Timestamp,Integer>> input, Collector<Tuple4<Integer,Timestamp,Timestamp,Integer>> out) throws Exception {
            Iterator<Tuple4<Integer,Timestamp,Timestamp,Integer>> iterator = input.iterator();
            Tuple4<Integer,Timestamp,Timestamp,Integer> first = iterator.next();
            String id = "";
            Long ts1 = 0L;
            Long ts2 = 0L;
            Integer pass = 0;
            if(first!=null){
                id = String.valueOf(first.f0);
                ts1 = first.f1.getTime();
                ts2 = first.f2.getTime();
                pass = first.f3;
            }
            while(iterator.hasNext()){
                Tuple4<Integer,Timestamp,Timestamp,Integer> next = iterator.next();
                if(next.f1.getTime() < ts1){
                    ts1 = next.f1.getTime();
                }
                if(next.f2.getTime() > ts2){
                    ts2 = next.f2.getTime();
                }
                pass += next.f3;
            }
            out.collect(new Tuple4<Integer,Timestamp,Timestamp,Integer>(Integer.parseInt(id), new Timestamp(ts1), new Timestamp(ts2), pass));
            
        }
    }
}
