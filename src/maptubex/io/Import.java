package maptubex.io;


//import java.io.*;
//import java.net.URI;

//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.fs.*;
//import org.apache.hadoop.fs.permission.FsPermission;
//import org.apache.hadoop.conf.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import maptubex.utils.Errors;


/**
 * Import a file into the distributed file system.
 * @author richard
 * @param uri The file to upload, possibly a file:// reference
 *
 *see: http://wiki.apache.org/hadoop/HadoopDfsReadWriteExample?action=AttachFile&do=view&target=HadoopDFSFileReadWrite.java
 *see: http://linuxjunkies.wordpress.com/2011/11/21/a-hdfsclient-for-hadoop-using-the-native-java-api-a-tutorial/
 */
public class Import {
//	static void usage () {
//		System.out.println("Usage : HadoopDFSFileReadWrite <inputfile> <output file>");
//		System.exit(1);
//	}


	
//	public static final void fromURI(final URI uri) throws IOException {
//		JobConf conf = new JobConf(Import.class);
//		//FileSystem fs = FileSystem.get(conf);
//		//FsPermission permission = new FsPermission();
//		//FileSystem.create(fs, "test", FsPermission.DEFAULT_UMASK);
//		
//		FileSystem fs = FileSystem.get(conf);
//		Path path = new Path("/test-file/myshapefile.shp");
//		OutputStream os = fs.create(path);
//		// write to os
//		os.close();
//	}
	
	/**
	 * Upload a file from the local file system.
	 * Basically a copy from:
	 * http://linuxjunkies.wordpress.com/2011/11/21/a-hdfsclient-for-hadoop-using-the-native-java-api-a-tutorial/
	 * @param inFilename The full path to the local file
	 * @param outfilename The full path on the remote file system
	 */
	public static final void fromLocal(final Configuration conf, final String inFilename, final String outFilename) throws IOException {
		//JobConf conf = new JobConf(Import.class);
		//Configuration conf = new Configuration();
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\core-site.xml"));
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\hdfs-site.xml"));
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\mapred-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		
		Path inFile = new Path(inFilename);
		Path outFile = new Path(outFilename);
		
		//validate
		if (!new File(inFilename).exists())
			Errors.printAndExit("Input file not found (local)");
		if (!new File(inFilename).isFile())
			Errors.printAndExit("Input should be a file (local)");
		//This is really a warning, but we're going to allow overwriting of existing files
		//if (fs.exists(outFile))
		//	utils.Errors.printAndExit("Output already exists (remote hdfs): "+outFilename);
		
		try {
			fs.copyFromLocalFile(inFile, outFile);
		}
		catch (IOException ioe) {
			System.out.println("Error writing file "+outFilename);
		}
	}
	
	/**
	 * Move a literal string into a file on the HDFS file system
	 * @param inString The string to write
	 * @param outFilename The filename to save the string to in HDFS
	 * @throws IOException
	 */
	public static final void fromString(final String inString, final String outFilename) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path outFile = new Path(outFilename);
		
		//validate
		//This is really a warning, but we're going to allow overwriting of existing files
		//if (fs.exists(outFile))
		//	utils.Errors.printAndExit("Output already exists (remote hdfs): "+outFilename);


		FSDataOutputStream out = fs.create(outFile);
		byte[] buf=new byte[10240];
		try {
			out.write(inString.getBytes("UTF-8")); //writeString writes double bytes
		}
		catch (IOException ioe) {
			System.out.println("Error writing file "+outFilename);
		}
		finally {
			out.close();
		}
	}
	
	/**
	 * Delete a file on the remote HDFS file system
	 * @param conf Configuration, required to get the correct file system
	 * @param remoteFilename
	 */
	public static final void deleteRemote(final Configuration conf, final String remoteFilename) throws IOException {
		//Configuration conf = new Configuration();
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\core-site.xml"));
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\hdfs-site.xml"));
		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\mapred-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		
		Path remoteFile = new Path(remoteFilename);
		boolean fileExists = fs.exists(remoteFile);
		//this is a warning, allow silent deletion of non-existent file or directory to succeed
		if (!fileExists) {
			//utils.Errors.printAndExit("Remote file does not exist: "+remoteFilename);
		}
		else { //file really exists, so generate errors if there is any problem deleting it
			try {
				//if (!fs.deleteOnExit(remoteFile)) { //this deletes the file when the JVM running the fs closes
				if (fs.delete(remoteFile,false)) { //this deletes the file immediately
					System.out.println("Error deleting file "+remoteFilename);
				}
			}
			catch (IOException ioe) {
				System.out.println("Error deleting file "+remoteFilename);
			}
		}
	}
	
	/**
	 * Delete a remote directory (RMR in hadoop dfs).
	 * I don't think you need this as deleteRemote works just fine
	 * @param remoteDirName
	 * @throws IOException
	 */
//	public static final void deleteRemoteRMR(final String remoteDirName) throws IOException {
//		Configuration conf = new Configuration();
//		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\core-site.xml"));
//		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\hdfs-site.xml"));
//		//conf.addResource(new Path("C:\\cygwin\\home\\richard\\hadoop-0.20.2\\conf\\mapred-site.xml"));
//		FileSystem fs = FileSystem.get(conf);
//		
//		Path remoteFile = new Path(remoteDirName);
//		if (!fs.exists(remoteFile))
//			printAndExit("Remote directory does not exist: "+remoteDirName);
//		try {
//			if (!fs.delete(remoteFile,true)) { //no idea what the "true" does! Need to look at docs.
//				System.out.println("Error deleting directory "+remoteDirName);
//			}
//		}
//		catch (IOException ioe) {
//			System.out.println("Error deleting directory "+remoteDirName);
//		}
//	}

}
