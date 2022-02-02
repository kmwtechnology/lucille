import com.kmwllc.lucille.core.Runner;

public class SpotifyIngestion {

  public static void main(String[] args) throws Exception {
    System.setProperty("config.file", "/Users/danieljung/Downloads/spotifyconf.conf");
    Runner.main(null);
  }

}
