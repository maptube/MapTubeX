package maptubex.functions;


import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;

//import FeatureWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;

//import org.geotools.data.shapefile.*;
//import org.geotools.data.shapefile.shp.*;
//import org.geotools.data.shapefile.ShapefileDataStore;
//import org.geotools.data.shapefile.ShapefileDataStoreFactory;
//import org.geotools.data.simple.SimpleFeatureIterator;
//import org.geotools.data.simple.SimpleFeatureSource;
//import org.geotools.feature.FeatureCollection;
//import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.simple.SimpleFeature;
//import org.opengis.feature.type.AttributeType;
//import org.opengis.feature.type.FeatureType;

import org.geotools.geometry.jts.JTS;
//import org.geotools.geometry.jts.JTSFactoryFinder;
//import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Geometry;
//import org.opengis.referencing.*;
//import org.opengis.referencing.crs.*;
//import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

//my imports
import maptubex.utils.Errors;
import maptubex.io.FeatureWritable;

public class Reproject extends Configured implements Tool {
	
//	public static final String GoogleProj =
//            "PROJCS[\"Popular Visualisation CRS / Mercator\","
//            + "GEOGCS[\"Popular Visualisation CRS\", DATUM[\"Popular Visualisation Datum\","
//            + "SPHEROID[\"Popular Visualisation Sphere\", 6378137, 0, AUTHORITY[\"EPSG\",\"7059\"]],"
//            + "TOWGS84[0, 0, 0, 0, 0, 0, 0], AUTHORITY[\"EPSG\",\"6055\"]],"
//            + "PRIMEM[\"Greenwich\", 0, AUTHORITY[\"EPSG\", \"8901\"]],"
//            + "UNIT[\"degree\", 0.0174532925199433, AUTHORITY[\"EPSG\", \"9102\"]],"
//            + "AXIS[\"E\", EAST], AXIS[\"N\", NORTH], AUTHORITY[\"EPSG\",\"4055\"]], PROJECTION[\"Mercator\"],"
//            + "PARAMETER[\"False_Easting\", 0], PARAMETER[\"False_Northing\", 0], PARAMETER[\"Central_Meridian\", 0],"
//            + "PARAMETER[\"Latitude_of_origin\", 0], UNIT[\"metre\", 1, AUTHORITY[\"EPSG\", \"9001\"]],"
//            + "AXIS[\"East\", EAST], AXIS[\"North\", NORTH], AUTHORITY[\"EPSG\",\"3785\"]]";
	
//	public static final String OSGB36Proj =
//            "PROJCS[\"OSGB 1936 / British National Grid\","
//            + "GEOGCS[\"OSGB 1936\","
//            + "DATUM[\"OSGB 1936\","
//            + "SPHEROID[\"Airy 1830\", 6377563.396, 299.3249646, AUTHORITY[\"EPSG\",\"7001\"]],"
//            + "TOWGS84[446.448, -125.157, 542.06, 0.15, 0.247, 0.842, -4.2261596151967575],"
//            + "AUTHORITY[\"EPSG\",\"6277\"]],"
//            + "PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]],"
//            + "UNIT[\"degree\", 0.017453292519943295],"
//            + "AXIS[\"Geodetic latitude\", NORTH],"
//            + "AXIS[\"Geodetic longitude\", EAST],"
//            + "AUTHORITY[\"EPSG\",\"4277\"]],"
//            + "PROJECTION[\"Transverse_Mercator\"],"
//            + "PARAMETER[\"central_meridian\", -2.0],"
//            + "PARAMETER[\"latitude_of_origin\", 49.0],"
//            + "PARAMETER[\"scale_factor\", 0.9996012717],"
//            + "PARAMETER[\"false_easting\", 400000.0],"
//            + "PARAMETER[\"false_northing\", -100000.0],"
//            + "UNIT[\"m\", 1.0],"
//            + "AXIS[\"Easting\", EAST],"
//            + "AXIS[\"Northing\", NORTH],"
//            + "AUTHORITY[\"EPSG\",\"27700\"]]";
//	
	
	/**
	 * Read the entire contents of a file into a string
	 * @return
	 */
	public String readFile(FileSystem fs, Path path) {
		StringBuffer result = new StringBuffer();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			while ((line=br.readLine())!=null) {
				result.append(line+"\n");
			}
			br.close();
		}
		catch (Exception ex) {
		}
		return result.toString();
	}

	
	/**
	 * Driver for MR reproject - main(String args[]) ?
	 * Need to work out the best way of passing in the source and dest CRS strings or objects? Maybe a MathTransform?
	 * @param args Arguments as follows below.
	 * @throws Exception
	 * args[0]="REPROJECT"
	 * args[1]=Source GeoSequence filename (HDFS)
	 * args[2]=Destination GeoSequence filename (HDFS)
	 * args[3]=Source Projection filename (HDFS)
	 * args[4]=Destination Projection filename (HDFS)
	 */
	public int run(final String args[]) throws Exception {
		//test args.length==4?
		if (args.length!=5) {
			Errors.printAndExit("Error: need 5 arguments");
		}
		for (int i=0; i<args.length; i++) System.out.println("arg"+i+"="+args[i]);
		//args[0]==REPROJECT
		String sourceFilename = args[1];
		String destFilename = args[2];
		String srcPRJFilename = args[3];
		String destPRJFilename = args[4];
		Path srcPRJPath = new Path(srcPRJFilename);
		Path destPRJPath = new Path(destPRJFilename);
		//Configuration conf = new Configuration();
		Configuration conf = super.getConf(); //this.getConf(); // super.getConf();
		System.out.println("mapred.job.tracker="+conf.get("mapred.job.tracker"));
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		//test that the reproject mapper is found
        try {
        	//Class c1 = Class.forName("maptubex.functions.Reproject$ReprojectMap");
        	//Class c1 = Class.forName("maptubex.functions.Reproject$ReprojectMap", true, Thread.currentThread().getContextClassLoader());
        	Class c1 = conf.getClassByName("maptubex.functions.Reproject$ReprojectMap");
        	ReprojectMap c2 = new ReprojectMap();
        	System.out.println("In Reproject Class found");
        	System.out.println("Class name="+c2.getClass().getName());
        }
        catch (Exception ex) {
        	System.out.println("In Reproject Class not found");
        }
		
		//test for files at args[0-3] locations?
		
		//need to set configuration parameters BEFORE assigning to the job
		//DistributedCache.addCacheFile(p.toUri(),job); //does this allow us to add a prj file?
		FileSystem fs = FileSystem.get(conf);
		String sourcePRJ = readFile(fs,srcPRJPath);
		String destPRJ = readFile(fs,destPRJPath);
		System.out.println("sourcePRJ="+sourcePRJ);
		System.out.println("destPRJ="+destPRJ);
		conf.set("sourceCRS", sourcePRJ);
		conf.set("destCRS", destPRJ);
		//You could save the WKT of the MathTransform?
		
		Job job = new Job(conf); //already done getConf() to get this
		System.out.println("Job JAR: "+job.getJar());
		//if the job jar is null, then it's getting the classes from the build path, not the jar
		job.setJarByClass(Reproject.class);
		job.setJobName("reproject");
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FeatureWritable.class);
	 
	    job.setMapperClass(ReprojectMap.class);
	    job.setCombinerClass(ReprojectReduce.class);
	    job.setReducerClass(ReprojectReduce.class);
	 
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	 
	    FileInputFormat.setInputPaths(job, new Path(sourceFilename));
	    Path outPath = new Path(destFilename);
	    FileOutputFormat.setOutputPath(job, outPath);
	    fs.delete(outPath, true); //delete any previous output before we try to create a new one and fail (hadoop dfs -rmr hrfd://localhost......)
		
		
		//TODO: need to split the shapefile into sequence files here...
		//...although this should really be a separate job
		//addDependingJob()
		
		job.waitForCompletion(true);

		//JobClient client = new JobClient();
		//client.setConf(conf);
		//try {
		//	JobClient.runJob(conf);
		//}
		//catch (Exception e) {
		//	e.printStackTrace();
		//}
		
		return 0;
	}
	
	//public void submitJob(String[] args) throws Exception {
	//	int res = ToolRunner.run(new Configuration(), new functions.Reproject(), args);
	//	System.exit(res);
	//}
	
	//public static void main(String[] args) throws Exception {
	//	int res = ToolRunner.run(new Configuration(), new Reproject(), args);
	//	System.exit(res);
	//}
	
//	private static final String[] DATA = {
//		"One, two, three",
//		"Four, five, six",
//		"Seven eight nine ten",
//		"Eleven, twelve, thirteen",
//		"Fourteen, fifteen, sixteen"
//	};
//	
//	public static void writeSequenceFile(String uri) throws IOException {
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		Path path = new Path(uri);
//		
//		IntWritable key = new IntWritable();
//		Text value = new Text();
//		SequenceFile.Writer writer = null;
//		try {
//			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),  value.getClass());
//			
//			for (int i=0; i<100; i++) {
//				key.set(100-i);
//				value.set(DATA[i%DATA.length]);
//				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
//				writer.append(key, value);
//			}
//			
//		}
//		finally {
//			IOUtils.closeStream(writer);
//		}
//	}
	
	
	
	
	

	//this is a zero reduce mapper from: http://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/Mapper.html
//	public static class Map extends Mapper(K key, V val, OutputCollector<K, V> output, Reporter reporter)
//			throws IOException {
//		// Process the <key, value> pair (assume this takes a while)
//        // ...
//        // ...
//        
//        // Let the framework know that we are alive, and kicking!
//        // reporter.progress();
//        
//        // Process some more
//        // ...
//        // ...
//        
//        // Increment the no. of <key, value> pairs processed
//        ++noRecords;
//
//        // Increment counters
//        reporter.incrCounter(NUM_RECORDS, 1);
//       
//        // Every 100 records update application-level status
//        if ((noRecords%100) == 0) {
//          reporter.setStatus(mapTaskId + " processed " + noRecords + 
//                             " from input-file: " + inputFile); 
//        }
//        
//        // Output the result
//        output.collect(key, val);
//      }
//    }
	
	
	//Mapper	
		//http://wiki.apache.org/hadoop/WordCount
		public static class ReprojectMap extends Mapper<Text, FeatureWritable, Text, FeatureWritable> {
			//private final static IntWritable one = new IntWritable(1);
			//private Text word = new Text();
			private static final FeatureWritable fw = new FeatureWritable();
			private Text fid = new Text();
			private static MathTransform transform;
			private static CoordinateReferenceSystem srcCRS;
			private static CoordinateReferenceSystem destCRS;
			
			
			public void setup(Context context) {
				System.out.println("setup");
				try {
					String sourceCRSWKT = context.getConfiguration().get("sourceCRS");
					String destCRSWKT = context.getConfiguration().get("destCRS");
					srcCRS = CRS.parseWKT(sourceCRSWKT);
					destCRS = CRS.parseWKT(destCRSWKT);
					boolean lenient = true; // allow for some error due to different datums
					transform = CRS.findMathTransform(srcCRS, destCRS, lenient);
				}
				catch (org.opengis.referencing.FactoryException fe) {
				}
				//catch (org.opengis.referencing.operation.TransformException te) {	
				//}
			}
			
			
			public void map(Text key, FeatureWritable value, Context context) throws IOException, InterruptedException {
				System.out.println("mapping: "+key.toString());
				fid.set(key);
				SimpleFeature feature = value.get();
				
				try {
					//need to reproject feature and construct a new FeatureWritable
					Geometry geometry = (Geometry) feature.getDefaultGeometry();
					Geometry transGeometry = JTS.transform(geometry, transform);
					feature.setDefaultGeometry(transGeometry);
				
					fw.set(feature);
					context.write(fid,fw);
				}
				//catch (org.opengis.referencing.FactoryException fe) {
				//}
				catch (org.opengis.referencing.operation.TransformException te) {	
				}
			}
		}

	
	//Reducer
		public static class ReprojectReduce extends Reducer<Text, FeatureWritable, Text, FeatureWritable> {
			//TODO: this is a simple collector pattern, so you don't necessarily need a reducer - can this be included
			//into the mapper using the right options?
			//Also, look at some of the overloads for reduce - is there an output collector one that is more appropriate?
			
			public void reduce(Text key, Iterable<FeatureWritable> values, Context context) 
					throws IOException, InterruptedException {
				for (FeatureWritable value : values) {
					context.write(key, value);
				}
			}
		}

	
	


}
