## Prerequisites of the Lucille JLama plugin

The Jlama plugin requires Java 20 and above. If you would like to use Lucille with a lower Java version without Jlama, follow this one time setup of Maven Toolchains. This setup ensures that a higher Java version is used only for the modules that require it.

## Maven Toolchains Setup

Note that the Jlama pom file is configured to automatically detect Java 21 SDKs. Change the version in the pom file if you are using Java 20 or higher than Java 21.

1. Download your Java version from your favourite vendor.
2. Add this to your `~/.m2/toolchains.xml` file:

```
<toolchains>
    <toolchain>
        <type>jdk</type>
        <provides>
            <version>{java.version.used.in.jlama.pom}</version>
        </provides>
        <configuration>
            <jdkHome>{path.to.jdk.home}</jdkHome>
        </configuration>
    </toolchain>
</toolchains>
```
 

