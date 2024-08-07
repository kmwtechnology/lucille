import com.kmwllc.lucille.core.Runner;

public class MongoDBConnectorTest {

	public static void main(String[] args) throws Exception {
		
		// java -Dconfig.file=conf/joining-db-connector-example.conf -cp 'lib/*' com.kmwllc.lucille.core.Runner -local
		// String[] args = new String[]{"-Dconfig.file=conf/joining-db-connector-example.conf -cp 'lib/*' com.kmwllc.lucille.core.Runner -local"};
	    
		// System.setProperty("config.file", "src/test/resources/MongoDBTest/geo_data.conf");
	    System.setProperty("config.file", "src/test/resources/MongoDBTest/mongo.conf");
	    // String[] args1 = new String[] {"conf/joining-db-connector-example.conf"};
	    Runner.main(args);	
		
	    
	    
	}

}
