echo "Maven Building"
mvn -pl '!plugin-kylin,!ranger-kylin-plugin-shim' -DskipJSTests -DskipTests=true clean package

echo "[DEBUG listing distro/target"
ls distro/target

echo "[DEBUG] listting local directory"
ls