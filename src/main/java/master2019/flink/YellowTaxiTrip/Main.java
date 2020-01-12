package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;


import java.sql.Timestamp;
import org.apache.flink.api.common.functions.MapFunction;
import java.io.File;;

public class Main {


    public static void main(String[] args) throws Exception{

        final ParameterTool params = ParameterTool.fromArgs(args);

        //Deleting output file to avoid getting error that the defined file already exists
        try{
            File f= new File(params.get("output")); 
            if(f.delete()){  
                System.out.println(f.getName() + " deleted");   //getting and printing the file name  
            }  
            else{  
                System.out.println("failed");  
            }  
        } catch (Exception e) {
            System.out.println(e);
        } 

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data
        DataStream<String> text;

        // read the text file from given input path
        text = env.readTextFile(params.get("input"));


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Tuple18<Integer, Long,Long,Integer,Float,Integer,String,Integer,Integer,Integer,Float,Float,Float,Float,Float,Float,Float,Float>> mapStream = text.
                map(new MapFunction<String, Tuple18<Integer, Long,Long,Integer,Float,Integer,String,Integer,Integer,Integer,Float,Float,Float,Float,Float,Float,Float,Float>>(){
                    public Tuple18 <Integer, Long,Long,Integer,Float,Integer,String,Integer,Integer,Integer,Float,Float,Float,Float,Float,Float,Float,Float>
                        map(String in){
                        String[] fieldArray = in.split(",");
                        Tuple18<Integer, Long,Long,Integer,Float,Integer,String,Integer,Integer,Integer,Float,Float,Float,Float,Float,Float,Float,Float>
                            out = new Tuple18(
                                Integer.parseInt(fieldArray[0]),    //VendorID
                                new Long(Timestamp.valueOf(fieldArray[1]).getTime()),   //tpep_pickup_datetime
                                new Long(Timestamp.valueOf(fieldArray[2]).getTime()),   //tpep_dropoff_datetime
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
                        return out;
                    }
                });

        SingleOutputStreamOperator<Tuple4<Integer,Long,Long,Integer>> result = JFKAlarms.airportTrips(mapStream);
        
        if (params.has("output")) {
            result.writeAsText(params.get("output"));
        }

        env.execute("Main");
       
    
    }

 
}
