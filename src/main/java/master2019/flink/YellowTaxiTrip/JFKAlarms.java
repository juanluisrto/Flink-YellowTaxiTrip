package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import java.sql.Timestamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Collector;

import scala.annotation.meta.getter;

import java.util.Iterator;



/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {
    AscendingTimestampExtractor<Tuple4<Integer,Timestamp,Timestamp,Integer>> tsExtractor;

    public JFKAlarms() {

    }

    public static SingleOutputStreamOperator airportTrips(SingleOutputStreamOperator input){

        //First we keep only the variables that we need for this task
        SingleOutputStreamOperator<JFKAlarmsEvent5> mapStream = input.
                map(new MapFunction<Event, JFKAlarmsEvent5>() {
                    public JFKAlarmsEvent5 map(Event in){
                        Tuple5<Integer,Timestamp,Timestamp,Integer,Integer> out = new Tuple5(
                                in.getTuple().getField(0),
                                in.getTuple().getField(1),
                                in.getTuple().getField(2),
                                in.getTuple().getField(3),
                                in.getTuple().getField(5)
                        );
                        return new JFKAlarmsEvent5(out);
                    }
                });

        SingleOutputStreamOperator<JFKAlarmsEvent5> filterStream = mapStream.
                filter(new FilterFunction<JFKAlarmsEvent5>() {
                    public boolean filter(JFKAlarmsEvent5 in) throws Exception{
                        Integer numPassenger = in.getTuple().f3;
                        if(numPassenger >= 2){
                            return in.getTuple().f4.equals(2);
                        }
                        else{
                            return false;
                        }
                    }
                });

        SingleOutputStreamOperator<JFKAlarmsEvent4> mapStream1 = filterStream.
                map(new MapFunction<JFKAlarmsEvent5,JFKAlarmsEvent4>() {
                    public JFKAlarmsEvent4 map(JFKAlarmsEvent5 in){
                        Tuple4<Integer,Timestamp,Timestamp,Integer> out = new Tuple4(
                                in.getTuple().getField(0),
                                in.getTuple().getField(1),
                                in.getTuple().getField(2),
                                in.getTuple().getField(3)
                        );
                        return new JFKAlarmsEvent4(out);
                    }
                });

        AscendingTimestampExtractor<JFKAlarmsEvent4> tsExtractor = new AscendingTimestampExtractor<JFKAlarmsEvent4>() {
            @Override
            public long extractAscendingTimestamp(JFKAlarmsEvent4 in) {
                return in.getTuple().f1.getTime();
            }
        };

        KeySelector<JFKAlarmsEvent4, Tuple1<Integer>> keySelector = new KeySelector<JFKAlarmsEvent4, Tuple1<Integer>> (){
            public Tuple1<Integer> getKey(JFKAlarmsEvent4 t) throws Exception {
                return new Tuple1(t.getTuple().f0); //Groups by Vendor Id
            }
        };

        KeyedStream<JFKAlarmsEvent4,Tuple1<Integer>> keyedStream = mapStream1.
                assignTimestampsAndWatermarks(tsExtractor).
                keyBy(keySelector);

        SingleOutputStreamOperator<Tuple4<Integer,Timestamp,Timestamp,Integer>> windowStream = keyedStream.
                window(TumblingEventTimeWindows.of(Time.hours(1))).apply(new PassengerSum());

        return windowStream;
    }



    public static class PassengerSum implements WindowFunction<JFKAlarmsEvent4, Tuple4<Integer,Timestamp,Timestamp,Integer>, Tuple1<Integer>, TimeWindow> {
        public void apply(Tuple1<Integer> tuple, TimeWindow timeWindow, Iterable<JFKAlarmsEvent4> input, Collector<Tuple4<Integer,Timestamp,Timestamp,Integer>> out) throws Exception {
            Iterator<JFKAlarmsEvent4> iterator = input.iterator();
            JFKAlarmsEvent4 first = iterator.next();
            Integer id = 0;
            Long ts1 = 0L;
            Long ts2 = 0L;
            Integer pass = 0;
            if(first!=null){
                id = first.getTuple().f0;
                ts1 = first.getTuple().f1.getTime();
                ts2 = first.getTuple().f2.getTime();
                pass = first.getTuple().f3;
            }
            while(iterator.hasNext()){
                JFKAlarmsEvent4 next = iterator.next();
                if(next.getTuple().f1.getTime() < ts1){
                    //We get the earlier timestamp of the earliest trip
                    ts1 = next.getTuple().f1.getTime();
                }
                if(next.getTuple().f2.getTime() > ts2){
                    //We do the same but for the lastest trip
                    ts2 = next.getTuple().f2.getTime();
                }
                //For each record we add the number of passengers
                pass += next.getTuple().f3;
            }
            out.collect(new Tuple4<Integer,Timestamp,Timestamp,Integer>(id, new Timestamp(ts1), new Timestamp(ts2), pass));

        }
    }
}

class JFKAlarmsEvent5 {

    Tuple5<Integer,Timestamp,Timestamp,Integer,Integer> tuple5;

    public JFKAlarmsEvent5(Tuple5<Integer,Timestamp,Timestamp,Integer,Integer> t){
        tuple5 = t;
    }

    public Tuple5<Integer,Timestamp,Timestamp,Integer, Integer> getTuple(){
        return tuple5;
    }
}

class JFKAlarmsEvent4 {

    Tuple4<Integer,Timestamp,Timestamp,Integer> tuple4;

    public JFKAlarmsEvent4(Tuple4<Integer,Timestamp,Timestamp,Integer> t){
        tuple4 = t;
    }

    public Tuple4<Integer,Timestamp,Timestamp,Integer> getTuple(){
        return tuple4;
    }
}