package maptubex.io;


import java.io.IOException;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.Writable;

import org.opengis.feature.simple.*;
import org.opengis.feature.type.*;
import com.vividsolutions.jts.io.OutStream;
import com.vividsolutions.jts.io.InStream;
import com.vividsolutions.jts.io.OutputStreamOutStream;
import com.vividsolutions.jts.io.InputStreamInStream;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.io.WKTReader;
import org.geotools.geometry.jts.JTS;
import com.vividsolutions.jts.geom.Geometry;
import org.opengis.feature.Property;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.FeatureTypeFactory;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;


/**
 * FeatureWritable class which wraps a geotools Feature to allow for serialisation/deserialisation in Hadoop 
 * @author richard
 *
 *It is necessary to implement the various comparable interfaces?
 */
public class FeatureWritable implements Writable {
	// Some data
	//private int counter;
	//private long timestamp;
	private SimpleFeature feature;
	//private SimpleFeatureType featureType;
	
	/**
	 * Overloaded constructor to create a blank class
	 */
	public FeatureWritable() {
	}
	
	/**
	 * Setter method for a simple feature
	 * @param simple
	 */
	public void set(SimpleFeature simple) {
		this.feature = simple;
	}
	
	/**
	 * Getter method for the simple feature
	 * @return The feature (simple variety)
	 */
	public SimpleFeature get() {
		return this.feature;
	}
	
	/**
	 * Need to have feature type set from shapefile schema before trying to deserialise the data.
	 * @param ft
	 */
	//public void setFeatureType(SimpleFeatureType ft) {
	//	this.featureType = ft;
	//}
	
	
	public void write(DataOutput out) throws IOException {
		//This method of serialising a feature is wasteful of space as the feature type is encoded into every row of data.
		//On the other hand, the data needs to be unstructured and self-describing, otherwise we end up passing the
		//FeatureType around every parallel instance. The attribute data is small in comparison to the geometry anyway, so
		//the overhead can't be massive.
		
		//out.writeInt(counter);
		//out.writeLong(timestamp);
		
		//String encodedType = DataUtilities.encodeType(feature.getFeatureType());
		//System.out.println(encodedType);
		//String names[] = DataUtilities.attributeNames(feature.getType());
		//for (int i=0; i<names.length; i++) System.out.println("Name="+names[i]);
		//String encoded = DataUtilities.encodeFeature(feature);
		//System.out.println("encoded="+encoded);
		
		//write feature ID first
		out.writeUTF(feature.getID());
		
		//then the encoded feature type - this is a string as follows:
		//the_geom:MultiPolygon,FIPS:String,ISO2:String,ISO3:String,UN:Integer,NAME:String,AREA:Integer,POP2005:java.lang.Long,REGION:Integer,SUBREGION:Integer,LON:Double,LAT:Double
		String encodedType = DataUtilities.encodeType(feature.getFeatureType());
		out.writeUTF(encodedType);
		
		//now write out all the attribute data
		for (int i=0; i<feature.getAttributeCount(); i++) {
			//Property a = attributes[i];
			Object a = feature.getAttribute(i);
			AttributeDescriptor desc = feature.getFeatureType().getDescriptor(i);
			//NO! this is FIPS! String typeName = desc.getType().getName().toString();
			//NO! Class cl = desc.getType().getClass();
			//AttributeType at = desc.getType();
			Class cl = desc.getType().getBinding(); //why on earth did they call it getBinding? It's the attribute class.
			//String typeName = cl.getSimpleName(); //give you String if you need the simple class name
			if (cl.equals(Long.class)) {
				out.writeLong((Long)a);
			}
			else if (cl.equals(Integer.class)) {
				out.writeInt((Integer)a);
			}
			else if (cl.equals(Double.class)) {
				out.writeDouble((Double)a);
			}
			else if (cl.equals(Float.class)) {
				out.writeFloat((Float)a);
			}
			else if (cl.equals(String.class)) {
				out.writeUTF((String)a);
			}
			//note skipping of any geometry descriptor here...
		}

		//TODO: switched this to write WKT, not WKB
		//finally, write out the geometry in binary
		WKBWriter writer = new WKBWriter();
		Geometry the_geom = (Geometry)feature.getDefaultGeometry();
        //Geometry geometry2 = JTS.transform(geometry, transform);
		//DataOutputStream stream = new DataOutputStream(out);
		//This doesn't work writing directly to the stream as it seems to read the wrong amount. Had to fix this by writing
		//the size of the geometry as a long first so we know the size, then using readbytes to get it back.
		//writer.write(the_geom, new OutputStreamOutStream((DataOutputStream)out)); //might be easier to use byte buffers?
		byte bufr[] = writer.write(the_geom);
		out.writeLong(bufr.length);
		out.write(bufr);
		//out.writeBytes("GUARD\n");
		
		//wkt version
		//WKTWriter writer = new WKTWriter();
		//String geom_str = writer.write(the_geom);
		//out.writeBytes(geom_str);
		//out.writeBytes("\n");
	}
	
	public void readFields(DataInput in) throws IOException {
		//counter = in.readInt();
		//timestamp = in.readLong();
		
		//read feature ID first
		String fID = in.readUTF();
		
		//read encoded feature type next
		try {
			String encodedFeatureType = in.readUTF();
			SimpleFeatureType ft = DataUtilities.createType("FeatureWritable", encodedFeatureType);
		
			//then all the field names
			int count = ft.getDescriptors().size();
			PropertyDescriptor props[]=ft.getDescriptors().toArray(new PropertyDescriptor[count]);
			Object a[] = new Object[count]; //create object array to load data into
			for (int i=0; i<count; i++) {
				PropertyType propT = props[i].getType();
				Class cl = propT.getBinding();
				if (cl.equals(Long.class)) {
					a[i]=in.readLong();
				}
				else if (cl.equals(Integer.class)) {
					a[i]=in.readInt();
				}
				else if (cl.equals(Double.class)) {
					a[i]=in.readDouble();
				}
				else if (cl.equals(Float.class)) {
					a[i]=in.readFloat();
				}
				else if (cl.equals(String.class)) {
					a[i]=in.readUTF();
				}
				//note skipping of any geometry descriptor here...
			}

			//and finally read the geometry
			Geometry the_geom=null;
			try {
				WKBReader reader = new WKBReader();
				//the_geom = reader.read(new InputStreamInStream((DataInputStream)in));
				long size = in.readLong();
				byte bufr[]=new byte[(int)size]; //TODO: why do you need to cast this to an int? potential problem? Use in instead (that's 2GB contiguous)
				in.readFully(bufr);
				the_geom = reader.read(bufr);
				//String guard = in.readLine();
				//if (!guard.equals("GUARD")) System.out.println("GUARD FAILED!");
				
				//wkt version
				//String geom_str = in.readLine();
				//WKTReader reader = new WKTReader();
				//the_geom = reader.read(geom_str);
				
			}
			catch (com.vividsolutions.jts.io.ParseException pe ) {
				System.out.println("Parse Exception");
			}
		
			//now create a feature with this feature type
			//SimpleFeatureTypeBuilder ftbuilder = new SimpleFeatureTypeBuilder();
			//ftbuilder.setSuperType(ft);
			//SimpleFeature f = ftbuilder.buildFeature();
			SimpleFeatureBuilder builder=new SimpleFeatureBuilder(ft);
			SimpleFeature f = builder.buildFeature(fID);
			f.setAttributes(a);
			f.setDefaultGeometry(the_geom);
			this.feature = f;
		}
		catch (SchemaException se) {
			System.out.println("Schema Exception");
		}
	}
	
	public static FeatureWritable read(DataInput in) throws IOException {
		FeatureWritable f = new FeatureWritable();
		f.readFields(in);
		return f;
	}

}
