package maptubex.io;


import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.opengis.feature.simple.SimpleFeature;

public class GeoSequenceFile {
	/**
	 * Load a local file into HDFS, turning it into a sequence file using a custom sequence file writer.
	 * Key is the string FID and value is the serialised feature.
	 * @param conf
	 * @param localFile
	 * @param remoteUri
	 * @throws IOException
	 */
	public static void writeGeoSequenceFile(final Configuration conf, String localFile, String remoteUri) throws IOException {
		//Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(remoteUri);
		
		Text key = new Text();
		FeatureWritable value = new FeatureWritable();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			
			try {
				ShapefileDataStore inStore = new ShapefileDataStore(new File(localFile).toURI().toURL());
				String name = inStore.getTypeNames()[0];
				SimpleFeatureSource inSource = inStore.getFeatureSource(name);
				FeatureCollection inFSShape = inSource.getFeatures();
				//SimpleFeatureType inFT = inSource.getSchema();
				SimpleFeatureIterator fIT = (SimpleFeatureIterator)inFSShape.features();
				while (fIT.hasNext())
				{
					SimpleFeature feature = fIT.next();
					key.set(feature.getID());
					value.set(feature);
					System.out.printf("[%s]\t%s\n", writer.getLength(), key);
					writer.append(key,value);
				}
				fIT.close();
			}
			//catch (java.net.URISyntaxException ue) {
			//	ue.printStackTrace();
			//}
			catch (java.io.IOException ioe) {
				ioe.printStackTrace();
			}
			
		}
		finally {
			IOUtils.closeStream(writer);
		}
	}

}
