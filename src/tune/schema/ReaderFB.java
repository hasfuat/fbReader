package tune.schema;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.*;

import org.apache.commons.io.IOUtils;







import tune.schema.rawLog;
//import com.google.flatbuffers.FlatBufferBuilder;

public class ReaderFB{
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws FileNotFoundException, IOException{
    	//private static String driverName = "org.apache.hive.jdbc.HiveDriver";
        File file = new File("/home/fuat/Documentation/flatBuffer/rawLog");  
    	readFile(file);

}

	/**
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void readFile(File file) throws FileNotFoundException, IOException {
		byte[] data = null;
		Obj_ID theObject= null;
      //File file = new File("/home/fuat/Documentation/flatBuffer/rawLog2015032014.ral0");


		   // byte[] data = null;
		    //InputStream file =  new InputStream( new BufferReader(new File("/home/fuat/2015/03/21/shard14_20150321_rev0_prison00_batcher00_seq0.fb.gz")));
		    DataInputStream stream = new DataInputStream(
		    	    new BufferedInputStream(new FileInputStream(file)));
		   data= IOUtils.toByteArray(stream);
		    
		    //data = ByteStreams.toByteArray(file);
		   System.out.println(data);

		    ByteBuffer bb = ByteBuffer.wrap(data);

		    while (bb.hasRemaining()) {
		    	int size = bb.getInt(); 
		    	byte[] record = new byte[size];
		    	bb.get(record);
		        theObject = printData(record);
		       // System.out.println(theObject);
		        System.out.println(theObject.key+" " +theObject.value);

		    }
	}

	private static Obj_ID printData(byte[] record) {
		
		/*Connection con = null;
		Statement st = null;

		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			con = DriverManager.getConnection(
					"jdbc:hive2://localhost:10000/default", "hadoop", "");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			st = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String table = "load_FB_files";
		String c = "create table if not exists test_hive(date string, value string) row format delimited fields terminated by ','";

		try {
			st.execute(c);
		} catch (SQLException e) {
			System.err.println("Cannot execute the query: "+e);
		}
		System.out.println("Table created successfully");*/
		// load data inpath '/authoritative/output.csv' into table test_hive;
		//String query = "load data inpath '/authoritative/output.csv' into table test_hive";
		
		
		
			ByteBuffer bb = ByteBuffer.wrap(record);
			rawLog rlog = tune.schema.rawLog.getRootAsrawLog(bb);
			Obj_ID theObject = new Obj_ID("Advertiser_ID",rlog.advertiserId());
			//System.out.println(rlog.advertiserId());
			//System.out.println(rlog.adNetworkId()+"  "+rlog.created()+" "+ rlog.clickCreated()+"  "+rlog.advertiserSubAdgroup()+" "+rlog.attributedIdDate());
			
			/*String insertQuery = "insert into table load_FB_files select forntendvalue1,frontendvalue2";	
			int q = 0;
			try {
				q = st.executeUpdate(insertQuery);
			} catch (SQLException e) {
				System.out.println("Cannot execute the query: " +e);
			}
			System.out.println("Boolean q :" + q);*/
			
			return theObject;
	}
}
