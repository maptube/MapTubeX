/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.ucl.casa.maptubex.reproject;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.net.URL;
import java.net.URI;
import java.util.Map;
import java.io.StringWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Random;

import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
//import org.geotools.data.DefaultFeatureResults;
import org.geotools.feature.FeatureCollection;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.simple.SimpleFeature;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;

import org.geotools.geojson.GeoJSON;
import org.geotools.geojson.GeoJSONUtil;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Envelope;

import java.io.IOException;
import org.opengis.referencing.FactoryException;

/**
 *
 * @author richard
 */
public class Reproject {

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    // TODO code application logic here
  }
  
  public static void reprojectShapefile(String inFilename, String outFilename, String outCRSWKT) {
    File inFile = new File(inFilename);
    File outFile = new File(outFilename);
    //URI inShapeURI = inFile.toURI();
    //URI outShapeURI = outFile.toURI();
    try {
      ShapefileDataStore inStore = new ShapefileDataStore(inFile.toURI().toURL());
      String name = inStore.getTypeNames()[0];
      SimpleFeatureSource inSource = inStore.getFeatureSource(name);
      FeatureCollection inFSShape = inSource.getFeatures();
      SimpleFeatureType inFT = inSource.getSchema();
      
      //build a transformation between the source and dest CRS
      CoordinateReferenceSystem srcCRS = inFT.getCoordinateReferenceSystem();
      //CoordinateReferenceSystem worldCRS = map.getCoordinateReferenceSystem();
      CoordinateReferenceSystem destCRS = CRS.parseWKT(outCRSWKT);
      boolean lenient = true; // allow for some error due to different datums
      //TODO: used to be able to print a warining if the datum shift was lenient
      MathTransform transform = CRS.findMathTransform(srcCRS, destCRS, lenient);
      
      //build schema for the new file
      SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
      builder.setName(name);
      builder.setCRS(destCRS);
      builder.addAll(inFT.getAttributeDescriptors());
      // build the type
      final SimpleFeatureType FEATURETYPE = builder.buildFeatureType();
      
      //create new datastore
      ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
      Map<String, Serializable> params = new HashMap<>();
      params.put("url", outFile.toURI().toURL());
      params.put("create spatial index", Boolean.FALSE);
      ShapefileDataStore newDataStore = (ShapefileDataStore)dataStoreFactory.createNewDataStore(params);
      newDataStore.createSchema(FEATURETYPE);
      //SimpleFeatureSource outSource = newDataStore.getFeatureSource();
      
      //read through the existing shapefile feature by feature and write out the new one
      Transaction transaction = new DefaultTransaction("Reproject");
      FeatureWriter<SimpleFeatureType, SimpleFeature> writer = newDataStore.getFeatureWriter(name, transaction);
      SimpleFeatureIterator fIT = (SimpleFeatureIterator)inFSShape.features();
      try {
        while (fIT.hasNext())
        {
          SimpleFeature feature = fIT.next();
          SimpleFeature copy = writer.next();
          copy.setAttributes(feature.getAttributes());
          //do the reprojection
          Geometry geometry = (Geometry) feature.getDefaultGeometry();
          Geometry geometry2 = JTS.transform(geometry, transform);
          copy.setDefaultGeometry(geometry2);
          writer.write();
        }
      }
      finally {
        writer.close();
        fIT.close();
        transaction.commit();
        transaction.close();
      }
    }
    catch (java.io.IOException ioe) {
      ioe.printStackTrace();
    }
    catch (org.opengis.referencing.FactoryException fac) {
      fac.printStackTrace();
    }
    catch (org.opengis.referencing.operation.TransformException tex) {
      tex.printStackTrace();
    }
  }
}
