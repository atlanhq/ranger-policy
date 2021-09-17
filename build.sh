echo "Maven Building"
mvn -pl '!plugin-kylin,!ranger-kylin-plugin-shim' -DskipJSTests -DskipTests=true -Drat.skip=true clean compile package install

echo "[DEBUG] listing distro/target"
ls distro/target

echo "[DEBUG] listing distro"
ls distro