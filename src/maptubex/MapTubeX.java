package maptubex;

import java.net.*;


//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.*;
//import org.geotools.referencing.CRS;
//import org.opengis.referencing.crs.CoordinateReferenceSystem;

import maptubex.functions.*;



//Namenode: 50070
//Datanodes: 50075
//SecondaryNameNode: 50090
//BackupCheckpointNode: 50105
//Jobtracker: 50030
//Tasktrackers: 50060

//3rd party jars
//http://www.datasalt.com/2011/05/handling-dependencies-and-configuration-in-java-hadoop-projects-efficiently/

//READ THIS on class submission, classpath and build path problems with eclipse
//http://stackoverflow.com/questions/11236305/launch-a-mapreduce-job-from-eclipse
	

public class MapTubeX {
	//OK, this is basically a hack to make sure everthing gets the same configuration without worrying about paths, variables
	//or directly setting IP addresses in xml files.
	//This DOES NOT set any of the run configuration arguments though...
	//TODO: need some clever way of the user setting this
	public static Configuration getHadoopConfiguration() {
		Configuration conf = new Configuration();
		//these are really supposed to be in the core-site.xml and mapred-site.xml files, but setting directly overrides
		//conf.set("fs.default.name", "hdfs://localhost:9000");
		//System.out.println("original mapred.job.tracker="+conf.get("mapred.job.tracker"));
		//conf.set("fs.default.name", "hdfs://128.40.47.111:9000");
		//conf.set("mapred.job.tracker", "128.40.47.111:9001");
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-1.2.1\\conf\\core-site.xml"));
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-1.2.1\\conf\\hdfs-site.xml"));
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-1.2.1\\conf\\mapred-site.xml"));
		
		conf.set("fs.default.name", "hdfs://localhost:9000");
		//conf.set("mapred.job.tracker", "localhost:9001");
		conf.set("mapred.jar","/home/richard/git/MapTubeX/MapTubeX.jar");
		return conf;
	}
	
	//main test harness
	//apparently, you're supposed to add -cp /opt/mapr/hadoop/hadoop-0.20.2/conf to the class path so that when you
	//create a new configuration, it uses the conf files for the cluster.
	//This has been done by adding a class path variable in Eclipse, pointing to the conf directory
	public static void main(String[] args) {
		//io.Import.shapefile(new URI("file://"));
		//io.Import.fromLocal("/Users/richard/Documents/test.txt");
		
		//print project classpath
		ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader)cl).getURLs(); 
        for(URL url: urls){
        	System.out.println(url.getFile());
        }
        //end of print project classpath
        
        //test that the reproject mapper is found
        try {
        	Class c1 = Class.forName("maptubex.functions.Reproject$ReprojectMap");
        	System.out.println("Class found");
        }
        catch (Exception ex) {
        	System.out.println("Class not found");
        }

		
		try {
			//io.Import.fromLocal("C:\\cygwin\\home\\richard\\hadoop-test.txt", "hdfs://localhost:9000/user/richard/test.txt");
			//io.Import.deleteRemote("/user/richard/test.txt");
			//io.Export.debugAsRaw("hdfs://localhost:9000/user/richard/test.txt");
			
			//reproject
			//upload files
			//io.Import.fromLocal(
			//		"C:\\Users\\richard\\Desktop\\MapsToMake\\Shapefile\\TM_WORLD_BORDERS-0.2.shp",
			//		"hdfs://localhost:9000/user/richard/TM_WORLD_BORDERS-0.2.shp");
			//io.Import.fromLocal(
			//		"C:\\Users\\richard\\Desktop\\MapsToMake\\Shapefile\\TM_WORLD_BORDERS-0.2.dbf",
			//		"hdfs://localhost:9000/user/richard/TM_WORLD_BORDERS-0.2.dbf");
			//io.Import.fromLocal(
			//		"C:\\Users\\richard\\Desktop\\MapsToMake\\Shapefile\\TM_WORLD_BORDERS-0.2.prj",
			//		"hdfs://localhost:9000/user/richard/TM_WORLD_BORDERS-0.2.prj");
			//functions.Reproject.writeSequenceFile("hdfs://localhost:9000/user/richard/sequence");
			
			
			//functions.Reproject.readSequenceFile();
			
			//testing - delete sequence file
			//io.Import.deleteRemote("hdfs://localhost:9000/user/richard/sequence");
			
			//delete any previous output first
			//mpx.io.Import.deleteRemote("hdfs://localhost:9000/user/richard/sequence-out");
			//start the reprojection job
			//functions.Reproject.doMRReproject(
			//		"hdfs://localhost:9000/user/richard/sequence",
			//		"hdfs://localhost:9000/user/richard/sequence-out"
			//);
			
			//test output
			//io.Export.toShapefile("hdfs://localhost:9000/user/richard/sequence-out/part-r-*", "myshapefile.shp");
			
			//////////////FULL REPROJECT JOB
			
			
			
			//upload a source and destination projection file
			//io.Import.fromLocal(
			//		"C:\\Users\\richard\\Desktop\\MapsToMake\\Shapefile\\TM_WORLD_BORDERS-0.2.prj",
			//		"hdfs://localhost:9000/user/richard/srcCRS.prj");
			//CoordinateReferenceSystem destCRS = CRS.decode("EPSG:3857");
			//io.Import.fromString(destCRS.toWKT(),"hdfs://localhost:9000/user/richard/destCRS.prj");
			
			//make sure there is no output file directory from a previous run
			//io.Import.deleteRemote("hdfs://localhost:9000/user/richard/sequence-out");
			//TODO: you need this to complete before moving on to the next line and it's asynchronous!
			
			//run the reproject job
			//String[] reprojectargs = new String [] {
			//		"hdfs://localhost:9000/user/richard/sequence",
			//		"hdfs://localhost:9000/user/richard/sequence-out",
			//		"hdfs://localhost:9000/user/richard/srcCRS.prj",
			//		"hdfs://localhost:9000/user/richard/destCRS.prj"
			//};
			//int res = ToolRunner.run(new Configuration(), new Reproject(), reprojectargs);
			//System.exit(res);
			//functions.Reproject.main(
			//		new String [] {
			//				"hdfs://localhost:9000/user/richard/sequence",
			//				"hdfs://localhost:9000/user/richard/sequence-out",
			//				"hdfs://localhost:9000/user/richard/srcCRS.prj",
			//				"hdfs://localhost:9000/user/richard/destCRS.prj"
			//		}
			//);
			
			//clean up all the bits into a shapefile
			//TODO: need projection here...
			//io.Export.toShapefile("hdfs://localhost:9000/user/richard/sequence-out/part-r-*", "myshapefile.shp");
			
			//final clean up of all output files
			
			
			//full configuration - first argument is the operation
			//Import.deleteRemote("hdfs://localhost:9000/user/richard/sequence-out"); //delete any previous output first
			Configuration conf = getHadoopConfiguration();
			String op = args[0];
			if (op.equalsIgnoreCase("STORESHP")) { //copy shapefile from local to remote file systems
				//not a map reduce job this, just store a file on HDFS
				
				//TODO: need to detect the file extension and use the relevant method to push it to HDFS here
				//move a local shapefile to HDFS and convert into a sequence file so we can use MR on it
				//i.e.	args[1]="C:\\Users\\richard\\Desktop\\MapsToMake\\Shapefile\\TM_WORLD_BORDERS-0.2.shp"
				//		args[2]="hdfs://localhost:9000/user/richard/sequence"
				maptubex.io.GeoSequenceFile.writeGeoSequenceFile(conf,args[1],args[2]);
			}
			else if (op.equalsIgnoreCase("STORE")) { //copy file from local to remote file systems
				//not a map reduce job this, just store a file on HDFS
				//TODO: need to detect the file extension and use the relevant method to push it to HDFS here
				//move a local file to HDFS
				//i.e.	args[1]="C:\\Users\\richard\\Desktop\\MapsToMake\\Shapefile\\shapefile.prj"
				//		args[2]="hdfs://localhost:9000/user/richard/shapefile.prj"
				maptubex.io.Import.fromLocal(conf,args[1],args[2]);
			}
			else if (op.equalsIgnoreCase("EXPORTSHP")) {
				//export a sequence file as a shapefile args[1]=remote HDFS sequence file, args[2]=local file to write
				//TODO: need to have detection of different file types and export formats
				maptubex.io.Export.toShapefile(conf,args[1], args[2]);
			}
			else if (op.equalsIgnoreCase("DELETE")) {
				//delete a file from HDFS - if multiple files are passed as arguments then they are all deleted
				for (int i=1; i<args.length; i++)
					maptubex.io.Import.deleteRemote(conf,args[i]);
			}
			else if (op.equalsIgnoreCase("REPROJECT")) {
				//need to have loaded shapefile first, plus the two projection files
				//conf.addResource(new Path("c:\\users\\richard\\workspace\\MapTubeX\\MapTubeX.jar"));
				
				//int res = ToolRunner.run(conf, new mpx.Reproject(), args);
				int res = ToolRunner.run(conf, new Reproject(), args);
			}
			//and another one?
			//Cluster i.e. K-Means?
			//IDW - how?
			//Heatmap - is this IDW?
			//Contour - hard!
			//Spatial index?
			//TSP - on the entire UK road network
			//JENKS
			//SORT?
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}		


}
