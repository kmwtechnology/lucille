package com.kmwllc.lucille;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When configured to do so, handles the retrieval of configs from a specified preset <code>configDirectoryPath</code>.
 * Given a <code>configDirectoryPath</code>, reads Configs from any .json or .conf files it encounters in that path.
 */
public class PresetConfigHandler {

  public static final Logger log = LoggerFactory.getLogger(PresetConfigHandler.class);

  private static final Set<String> ACCEPTED_EXTENSIONS = Set.of("conf", "json", "hocon");

  private final Path configDirectoryPath;

  public PresetConfigHandler(Path configDirectoryPath) {
    this.configDirectoryPath = configDirectoryPath;
  }

  /**
   * Retrieves Configs from this handler's <code>configDirectoryPath</code>. Ignores files that are not <code>.conf</code>
   * or <code>.json</code>. Configs are keyed by their filename <i>including</i> extensions.
   * @return A mapping of filenames (including extensions) to the retrieved configs.
   * @throws IOException If the provided path is not a directory, or in the event of miscellaneous
   * IO errors during file reading.
   */
  public Map<String, Config> fetchConfigs() throws IOException {
    Map<String, Config> filenamesToConfigs = new HashMap<>();

    if (configDirectoryPath == null) {
      return filenamesToConfigs;
    }

    if (!Files.isDirectory(configDirectoryPath)) {
      throw new IOException(configDirectoryPath + " is not a directory");
    }

    try (Stream<Path> files = Files.list(configDirectoryPath)) {
      files
          .filter(Files::isRegularFile)
          .filter(path -> {
            String extension = FilenameUtils.getExtension(path.getFileName().toString());
            return ACCEPTED_EXTENSIONS.contains(extension);
          })
          .forEach(path -> {
            try {
              // for the purposes of actually RUNNING the API, a simple call to
              // .resolve() would suffice here. However, it does not work
              // with unit tests. This more explicit call works with both, appears
              // to have the same effect, and does not create disgustingly large configs.
              Config parsed = ConfigFactory.parseFile(path.toFile());
              Config resolved = ConfigFactory.defaultOverrides().withFallback(parsed).resolve();
              filenamesToConfigs.put(path.getFileName().toString(), resolved);
            } catch (ConfigException e) {
              log.warn("Error loading preset config at {}:", path, e);
            }
          });
    }

    return filenamesToConfigs;
  }
}
