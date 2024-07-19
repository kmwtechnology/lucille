export CONTAINERS=$(docker ps -a -q)
docker stop ${CONTAINERS}
docker rm ${CONTAINERS}
mvn clean install -DskipTests
docker build -t lucille-admin .
docker run -p 8080:8080 lucille-admin
