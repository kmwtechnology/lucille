
import com.kmwllc.lucille.core.Runner;

public class RunSalesforceIngest {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub

    String configFile = "./conf/contacts.conf";
    System.setProperty("config.file", configFile);
    Runner.main(args);
  }

}
