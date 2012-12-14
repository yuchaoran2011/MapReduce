// Name: Chaoran Yu
// Login: cs61c-bm
// SID: 22903110

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.lang.Math.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * This is the skeleton for CS61c project 1, Fall 2012.
 *
 * Contact Alan Christopher or Ravi Punj with questions and comments.
 *
 * Reminder:  DO NOT SHARE CODE OR ALLOW ANOTHER STUDENT TO READ YOURS.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */
public class Proj1 {

    /** An Example Writable which contains two String Objects. */
    public static class StringPair implements Writable {
        /** The String objects I wrap. */
	private String a, b;

	/** Initializes me to contain empty strings. */
	public StringPair() {
	    a = b = "";
	}
	
		/** Initializes me to contain A, B. */
        public StringPair(String a, String b) {
            this.a = a;
			this.b = b;
        }

        /** Serializes object - needed for Writable. */
        public void write(DataOutput out) throws IOException {
            new Text(a).write(out);
			new Text(b).write(out);
        }

        /** Deserializes object - needed for Writable. */
        public void readFields(DataInput in) throws IOException {
			Text tmp = new Text();
			tmp.readFields(in);
			a = tmp.toString();
	    
			tmp.readFields(in);
			b = tmp.toString();
			}

		/** Returns A. */
		public String getA() {
			return a;
		}
		/** Returns B. */
		public String getB() {
			return b;
		}
    }
	
	
	public static class DoublePair implements Writable {
		private Double a, b;
		
		public DoublePair() {
			a = new Double(0);
			b= new Double(0);
		}
		
		public DoublePair(double a, double b) {
			this.a = new Double(a);
			this.b = new Double(b);
		}
		
		/** Serializes object - needed for Writable. */
		public void write(DataOutput out) throws IOException {
			new DoubleWritable(a).write(out);
			new DoubleWritable(b).write(out);
		}
		
		/** Deserializes object - needed for Writable. */
		public void readFields(DataInput in) throws IOException {
			DoubleWritable tmp = new DoubleWritable();
			tmp.readFields(in);
			a = tmp.get();

			tmp.readFields(in);
			b = tmp.get();
		}
		
		/** Returns A. */
		public Double getA() {
			return a.doubleValue();
		}
		/** Returns B. */
		public Double getB() {
			return b.doubleValue();
		}
	}

/**
 *	map1: (gram, f(n))
 *  combine1: (gram, (Sw, G))
 *  reduce1: (gram, -cooccurrence rate)
 *  reduce2: (cooccurrence rate, gram)
 **/

 /** Inputs a set of (docID, document contents) pairs.
   * Outputs a set of (Text, Text) pairs.
   */
    public static class Map1 extends Mapper<WritableComparable, Text, Text, DoublePair> {
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");
		private Text word = new Text();

		private String targetGram = null;
		private int funcNum = 0;

        /*
         * Setup gets called exactly once for each mapper, before map() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to map
         */
        @Override
        public void setup(Context context) {
            targetGram = context.getConfiguration().get("targetGram").toLowerCase();
			try {
				funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
				} catch (NumberFormatException e) {
					/* Do nothing. */
				}
			}

        @Override
        public void map(WritableComparable docID, Text docContents, Context context)
		throws IOException, InterruptedException {
            Matcher matcher = WORD_PATTERN.matcher(docContents.toString());
			Func func = funcFromNum(funcNum);

			//Maybe read in the input?

			// Find number of words in tagetGram
			String targetWords[] = targetGram.split(" ");
			int targetNum = targetWords.length;
					
	
			
			
			// Put all individual words in a HashMap. Key, values pairs are (word position: word)
			HashMap<Integer, String> document= new HashMap<Integer, String>();
			int currentPos = 0;
			while (matcher.find()) {
				word.set(matcher.group().toLowerCase());
				document.put(new Integer(currentPos), word.toString());
				currentPos++;
			}
			
			
			currentPos = -1;
			String nGram = new String();
			
			
			/* Code below put all locations of targetGram in an ArrayList: targetPos
			 and put all locations of grams other than target gram in otherPos.
			 otherGram holds otherPos's corresponding string grams. */
			ArrayList<Integer> targetPos = new ArrayList<Integer>();
			ArrayList<Integer> otherPos = new ArrayList<Integer>();
			ArrayList<String> otherGram = new ArrayList<String>();
			
			Integer currentNum = new Integer(0);
			while (document.get(currentNum) != null){
				if (document.get(new Integer(currentNum + targetNum -1)) == null){
					break;
				}
				String currentGram = document.get(currentNum);
				for (int i=1; i<targetNum; i++) {
					currentGram = currentGram + " " + document.get(new Integer(currentNum + i));
				}
				if (currentGram.equals(targetGram)){
					targetPos.add(currentNum);
				}
				else{
					otherGram.add(currentGram);
					otherPos.add(currentNum);
				}
				currentNum = new Integer(currentNum + 1);
			}
			
			
			
			int dist;
			for (int i=0; i<otherPos.size(); i++) {
				int min = Integer.MAX_VALUE;
				if (targetPos.size() == 0) {
					dist = Integer.MAX_VALUE;
				}
				else {
					min = Math.abs(otherPos.get(i).intValue() - targetPos.get(0).intValue());
					for (int j=1; j<targetPos.size(); j++) {
						if (Math.abs(otherPos.get(i).intValue() - targetPos.get(j).intValue()) < min) {
							min = Math.abs(otherPos.get(i).intValue() - targetPos.get(j).intValue());
						}
					}
					dist = min;
				}
				double f_value = (dist == Integer.MAX_VALUE)?0:(func.f(dist));
				context.write((new Text(otherGram.get(i))), new DoublePair(f_value, 1));
			}
		}
		
		
		
			
	
	/** Returns the Func corresponding to FUNCNUM*/
	private Func funcFromNum(int funcNum) {
	    Func func = null;
	    switch (funcNum) {
	    case 0:	
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
			}			
		    };	
		break;
	    case 1:
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
			}			
		    };
		break;
	    case 2:
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : Math.sqrt(d);
			}			
		    };
		break;
	    }
	    return func;
	}
    }

    /** Here's where you'll be implementing your combiner. It must be non-trivial for you to receive credit. */
    public static class Combine1 extends Reducer<Text, DoublePair, Text, DoublePair> {

      @Override
      public void reduce(Text key, Iterable<DoublePair> values,
              Context context) throws IOException, InterruptedException {
		  double sg=0;
		  double num=0;
		  for (DoublePair dp: values) {
			  sg += dp.getA().doubleValue();
			  num += 1;
		  }
		  context.write(key, new DoublePair(sg, num));
      }
    }


		
	// The first reducer accepts inputs from the first mapper and accumulate Sw and G
	// It emits pairs of the form (gram, (Sw, G))
    public static class Reduce1 extends Reducer<Text, DoublePair, DoubleWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<DoublePair> values,
			   Context context) throws IOException, InterruptedException {
			double sg = 0.0;
			double num = 0.0;
			for (DoublePair dw: values) {
				sg += dw.getA().doubleValue();
				num += dw.getB().doubleValue();
			}
			double occurrenceRate = (sg>0.0)?((sg * Math.pow(Math.log(sg),3.0)) / num):0.0;
			context.write(new DoubleWritable(-occurrenceRate), key);
		}
	}

		
		
		
    public static class Map2 extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
    }
		
		
		

    public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

      int n = 0;
      static int N_TO_OUTPUT = 100;

      /*
       * Setup gets called exactly once for each reducer, before reduce() gets called the first time.
       * It's a good place to do configuration or setup that can be shared across many calls to reduce
       */
      @Override
      protected void setup(Context c) {
        n = 0;
      }

		// The second reducer accepts inputs of the form (gram, co-occurrence rate) from mapper 2 and 
		// output final output of the form (co-occurence rate, gram)
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
	    //you should be outputting the final values here.
			
			for (Text gram: values) {
				context.write(new DoubleWritable(Math.abs(key.get())), gram);
			}
        }
    }

    /*
     *  You shouldn't need to modify this function much. If you think you have a good reason to,
     *  you might want to discuss with staff.
     *
     *  The skeleton supports several options.
     *  if you set runJob2 to false, only the first job will run and output will be
     *  in TextFile format, instead of SequenceFile. This is intended as a debugging aid.
     *
     *  If you set combiner to false, neither combiner will run. This is also
     *  intended as a debugging aid. Turning on and off the combiner shouldn't alter
     *  your results. Since the framework doesn't make promises about when it'll
     *  invoke combiners, it's an error to assume anything about how many times
     *  values will be combined.
     */
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", true);

        if(runJob2)
          System.out.println("running both jobs");
        else
          System.out.println("for debugging, only running job 1");

        if(combiner)
          System.out.println("using combiner");
        else
          System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
          System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
          System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
          System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
          System.exit(1);
        }

        {
            Job firstJob = new Job(conf, "wordcount+co-occur");

            firstJob.setJarByClass(Map1.class);

	    /* You may need to change things here */
            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(DoublePair.class);
            firstJob.setOutputKeyClass(DoubleWritable.class);
            firstJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
              firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
              firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);

            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "sort");

            secondJob.setJarByClass(Map1.class);
	    /* You may need to change things here */
            secondJob.setMapOutputKeyClass(DoubleWritable.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            secondJob.setMapperClass(Map2.class);
            if(combiner)
              secondJob.setCombinerClass(Reduce2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }

}
