echo "Maven Building"
mvn -pl '!plugin-kylin,!ranger-kylin-plugin-shim,!plugin-yarn,!ranger-yarn-plugin-shim,!plugin-solr,!ranger-solr-plugin-shim,!plugin-kafka,!ranger-kafka-plugin-shim,!plugin-presto,!ranger-presto-plugin-shim' -DskipJSTests -DskipTests=true -Drat.skip=true clean package -Pall

echo "[DEBUG] listing distro/target"
ls distro/target