//hadoop fs -cp hdfs://nameservice1/user/hive/warehouse/lxr_tx_1601 xrli/cash
//hadoop jar Cash.jar  SortCardTime -Dmapreduce.job.queuename=root.default xrli/cash/lxr_tx_1601 xrli/cash/SortCardTime_tx_1601


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

 


public class SortCardTime {

	private static Logger logger = LoggerFactory
			.getLogger(SortCardTime.class);

	

	public static class UpdateMapper extends Mapper<LongWritable, Text, MultiSortUtil.KeyPair, Text> {

		public MultiSortUtil.KeyPair keyPair = new MultiSortUtil.KeyPair();
		private Text info = new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr=new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	String[] list = itr.nextToken().split("\\001");
            	String card= list[0];
            	String time = list[2] + list[3];
            	keyPair.set(card, time);
               
            	String money = list[1];
            	String newstr = card + " " + time + " " + money;
            	info.set(newstr);
    			context.write(keyPair, info);
            }
		}
	}

	public static class UpdateReducer extends Reducer<MultiSortUtil.KeyPair, Text, Text, Text> {
		private Text result = new Text();
		private static TreeMap<String, Double> Qmoney = new TreeMap<String, Double> ();
		
		@Override
		public void reduce(MultiSortUtil.KeyPair key, Iterable<Text> lines, Context context)
				throws IOException, InterruptedException {
		    String curtime ="";
		    int curDWeek = 0;
		    String curHour = "";
		    Double curM = 0.0;
		    String curTS =""; 
		    
		    String lasttime ="";
		    int lastDWeek = 0;
		    String lastHour = "";
		    Double lastM = 0.0;
		    String lastTS = "";
 
			for (Text info : lines) {
				  String[] list = info.toString().split("\\s");
				  String card = list[0];
	              curTS = list[1];
	              curM = Double.valueOf(list[2]);
				  
	              if(curTS.length()==14){	
	            		String tempDate = curTS.substring(0, 4) + "-" + curTS.substring(4, 6) + "-" + curTS.substring(6, 8); 
						String tempPeriod = curTS.substring(8, 10) + ":" + curTS.substring(10, 12) + ":" + curTS.substring(12, 14);
						curtime = tempDate + " " + tempPeriod;
						curHour = curTS.substring(8, 10);
	 
		 
					final Double pMoney = curM;
					
					Iterator<String> ir = Qmoney.keySet().iterator();
					
					while(ir.hasNext()){
						String pd = ir.next();
						String pdtime = pd.substring(0, 4) + "-" + pd.substring(4, 6) + "-" + pd.substring(6, 8) + " " +pd.substring(8, 10) + ":" + pd.substring(10, 12) + ":" + pd.substring(12, 14);
						if(TimeUtil.getDeltaMin(curtime, pdtime)/(60*24) > 7)
							ir.remove();
						else
							break;
					}
					     
					Double periodMSum = static_window.calTotal(Qmoney);	

					Qmoney.put(curTS,pMoney);
						
					DecimalFormat df = new DecimalFormat("######0.00"); 
					
					String DeltaT = null;

				    if(lasttime.equals("")){
				    	DeltaT = null;
				    }
		      		else{
		      		   	Double deltaTime = TimeUtil.getDeltaMin(curtime,lasttime);
				    	DeltaT = df.format(deltaTime);
		      		}
				    
				    
				    curDWeek = TimeUtil.DayofWeek(curTS.substring(0,8));
				  
					    		
				    
				    String tempInfo = card + "\t" + curTS + "\t" + curDWeek+ "\t" + curHour + "\t" + curM 
				    		+ "\t" + lastTS + "\t" + lastDWeek + "\t" + lastHour + "\t" + lastM
				    		+ "\t" + DeltaT + "\t" + periodMSum;
				    
				     
				    lasttime = curtime;
				    lastTS = curTS;
				    lastDWeek = curDWeek;
				    lastHour = curHour;
				    lastM = curM; 
			 
				    result.set(tempInfo);
				    context.write(result,new Text(""));
			 }
			}
			Qmoney.clear();
		}
	}
	
	 
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		 
		Configuration conf = new Configuration();
		//conf.set("mapreduce.job.queuename", "root.default");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf,"SortCardTime");

		job.setJarByClass(SortCardTime.class); // 设置运行jar中的class名称
		
		job.setPartitionerClass(MultiSortUtil.FirstPartitioner.class);
		job.setGroupingComparatorClass(MultiSortUtil.GroupingComparator.class);
		
		job.setMapperClass(UpdateMapper.class);// 设置mapreduce中的mapper reducer
		job.setReducerClass(UpdateReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(MultiSortUtil.KeyPair.class);
		job.setNumReduceTasks(100);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
	
	
}
