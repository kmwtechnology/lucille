# Entrypoint to start the server using a dummy lucille config

java -Dconfig.file=conf/simple-config.conf -jar target/lucille-admin-0.2.3-SNAPSHOT.jar server conf/admin.yml