package maptubex.io;


import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import org.opengis.feature.simple.*;
import org.opengis.feature.type.*;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.DataUtilities;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.data.FileDataStoreFactorySpi;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
//import org.geotools.data.shapefile.indexed.IndexedShapefileDataStoreFactory;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;


/**
 * Export a file from the distributed file system in a variety of formats
 * @author richard
 *
 */
public class Export {
	static void printAndExit(String str) {
		System.err.println(str);
		System.exit(1);
	}
	
	/**
	 * Read a remote file back from HDFS to the console for debugging purposes
	 * @param remoteFilename
	 * @throws IOException
	 */
	public static final void debugAsRaw(final String remoteFilename) throws IOException {
		Configuration conf = new Configuration();
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\core-site.xml"));
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\hdfs-site.xml"));
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\mapred-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		
		Path remoteFile = new Path(remoteFilename);
		
		//validate
		if (!fs.exists(remoteFile))
			printAndExit("Remote file not found (hdfs remote): "+remoteFilename);
		
		FSDataInputStream in = fs.open(remoteFile);
		
		byte[] buf=new byte[10240];
		try {
			int bytesRead=0;
			while ((bytesRead = in.read(buf, 0, buf.length))>0) {
				System.out.write(buf,0,bytesRead);
			}
		}
		catch (IOException ioe) {
			System.out.println("Error reading file "+remoteFilename);
		}
		finally {
			in.close();
		}
	}
	
	//collect MR outputs
	//hdfs.matchFiles("/user/kenny/mrjob/", "part-")
//	public List<Path> matchFiles(String path, final String filter) {
//		List<Path> matches = new LinkedList<Path>();
//		try {
//			FileStatus[] statuses = fileSystem.listStatus(new Path(path), new PathFilter() {
//				public boolean accept(Path path) {
//					return path.toString().contains(filter);
//				}
//			});
//			for(FileStatus status : statuses) {
//				matches.add(status.getPath());
//			}
//		}
//		catch(IOException e) {
//			LOGGER.error(e.getMessage(), e);
//		}
//        return matches;
//    }

	
	/**
	 * Export a shapefile from the Hadoop HDFS in the FeatureWritable format and store it on the local system.
	 * @param conf
	 * @param remoteInFilename
	 * @param localOutFilename
	 * TODO: you really need to force a CRS here as it's not stored in the FeatureWriter that we're reading from
	 */
	public static final void toShapefile(final Configuration conf, final String remoteInFilename, final String localOutFilename) throws IOException {
		//Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);
		
		Path inFile = new Path(remoteInFilename);
		//Path outFile = new Path(localOutFilename); //I'm using localOutFilename directly
		
		//validate
		FileStatus partfiles[] = fs.globStatus(inFile); //get list of all partfiles
		if (partfiles.length==0)
			printAndExit("files not found (remote hdfs): "+remoteInFilename);
		
		//set up shapefile writing
		FileDataStoreFactorySpi factory = new ShapefileDataStoreFactory();
		File file = new File(localOutFilename);
		Map<String, Serializable> params = new HashMap<String, Serializable>();
		params.put("url", file.toURI().toURL());
		params.put("create spatial index", Boolean.FALSE);
		ShapefileDataStore dataStore = (ShapefileDataStore)factory.createNewDataStore(params);
		//here's the problem: we don't know the feature type until we  load the first feature
		//String typeName = dataStore.getTypeNames()[0];
		//FeatureType ft = DataUtilities.createType(typeName, typeSpec)
		//FeatureSource source = dataStore( typeName );
		String typeName = null;
		SimpleFeatureType ft = null;
		//SimpleFeatureSource source = null;
		Transaction transaction = new DefaultTransaction("Write Shapefile");
		FeatureWriter<SimpleFeatureType, SimpleFeature> writer = null;

		//process all the partfiles
		for (FileStatus partfile : partfiles)
		{
			//FSDataInputStream in = fs.open(inFile);
			//FSDataOutputStream out = fs.create(outFile);
			SequenceFile.Reader reader = null;
			try {
				reader = new SequenceFile.Reader(fs, partfile.getPath(), conf);
				Text key = new Text();
				FeatureWritable value = new FeatureWritable();
				while (reader.next(key, value)) {
					SimpleFeature feature = value.get();
					if (ft==null) { //only if this is the first record, set up the feature type to write the shapefile
						ft = feature.getFeatureType();
						typeName = ft.getTypeName();
						dataStore.createSchema(ft);
						//source = dataStore.getFeatureSource(typeName);
						writer = dataStore.getFeatureWriter(typeName, transaction);
					}
					System.out.println("Writing "+feature.getID());
					SimpleFeature copy = writer.next();
					copy.setAttributes(feature.getAttributes());
					copy.setDefaultGeometry(feature.getDefaultGeometry());
					writer.write();
				}
			}
			finally {
				if (reader!=null) reader.close();
				if (writer!=null) writer.close();
				transaction.commit();
				transaction.close();
			}
		}
		
	}
	
	public static final void geoJSON() {
		
	}

}
