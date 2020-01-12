package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Date;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import java.io.File;;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;



/*
Commands to run prior to execution

//Initialize cluster
/usr/local/Cellar/apache-flink/1.9.1/libexec/libexec/start-cluster.sh

//compile project
mvn clean package

//run flink
flink run -c master2019.flink.YellowTaxiTrip.Main target/YellowTaxiTrip-1.0-SNAPSHOT.jar --input /Users/juanluisrto/Documents/Universidad/UPM/'Cloud Computing'/YellowTaxiTrip/yellow_tripdata_2019_06.csv
 */

public class Main {

    public static String OUT_LARGE_TRIPS =  "largeTrips.csv";
    public static String OUT_JFK_TRIPS =  "jfkTrips.csv";

    public static void main(String[] args) throws Exception{

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data
        DataStream<String> text;
        // read the text file from given input path

        String p = "/Users/juanluisrto/Documents/Universidad/UPM/Cloud Computing/YellowTaxiTrip/yellow_tripdata_2019_06mini.csv";
        String o = "/Users/juanluisrto/Documents/Universidad/UPM/Cloud Computing/YellowTaxiTrip/";
        //text = env.readTextFile(p);
        text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Event> mapStream = text.
                map(new MapFunction<String, Event>(){
                    public Event
                        map(String in){
                        String[] fieldArray = in.split(",");
                        Tuple18<Integer, Timestamp, Timestamp, Integer, Float, Integer, String, Integer, Integer, Integer, Float, Float, Float, Float, Float, Float, Float, Float>
                                out = new Tuple18(
                                Integer.parseInt(fieldArray[0]),    //VendorID
                                Timestamp.valueOf(fieldArray[1]),   //tpep_pickup_datetime
                                Timestamp.valueOf(fieldArray[2]),   //tpep_dropoff_datetime
                                Integer.parseInt(fieldArray[3]),    //passenger_count
                                Float.parseFloat(fieldArray[4]),    //trip_distance
                                Integer.parseInt(fieldArray[5]),    //RatecodeID,
                                fieldArray[6],                      //store_and_fwd_flag
                                Integer.parseInt(fieldArray[7]),    //PULocationID
                                Integer.parseInt(fieldArray[8]),    //DOLocationID
                                Integer.parseInt(fieldArray[9]),    //payment_type
                                Float.parseFloat(fieldArray[10]),    //fare_amount
                                Float.parseFloat(fieldArray[11]),    //extra
                                Float.parseFloat(fieldArray[12]),    //mta_tax
                                Float.parseFloat(fieldArray[13]),    //tip_amount
                                Float.parseFloat(fieldArray[14]),    //tolls_amount
                                Float.parseFloat(fieldArray[15]),    //improvement_surcharge
                                Float.parseFloat(fieldArray[16]),    //total_amount
                                Float.parseFloat(fieldArray[17])    //congestion_surcharge.


                                );
                        return new Event(out);
                    }
                });

        LargeTrips.initializer();
        SingleOutputStreamOperator<Tuple5<Integer, Date, Integer, Timestamp, Timestamp>> outputLargeTrips = LargeTrips.run(mapStream);
        outputLargeTrips.writeAsText(params.get("output") + OUT_LARGE_TRIPS, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        SingleOutputStreamOperator<Tuple4<Integer,Timestamp,Timestamp,Integer>> outputJFKTrips = JFKAlarms.airportTrips(mapStream);
        
        if (params.has("output")) {
            outputJFKTrips.writeAsText(params.get("output")+ OUT_JFK_TRIPS, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }
        env.execute("Main");
    }
}

class Event {

    Tuple18<Integer, Timestamp, Timestamp, Integer, Float, Integer, String, Integer, Integer, Integer, Float, Float, Float, Float, Float, Float, Float, Float> tuple18;

    public Event(Tuple18<Integer, Timestamp, Timestamp, Integer, Float, Integer, String, Integer, Integer, Integer, Float, Float, Float, Float, Float, Float, Float, Float> t) {
        tuple18 = t;
    }

    public Tuple18<Integer, Timestamp, Timestamp, Integer, Float, Integer, String, Integer, Integer, Integer, Float, Float, Float, Float, Float, Float, Float, Float> getTuple(){
        return tuple18;
    }
}




