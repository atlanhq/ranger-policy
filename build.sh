echo "Maven Building"
mvn -DskipJSTests -DskipTests=true clean package

echo "[DEBUG listing distro/target"
ls distro/target

echo "[DEBUG] listting local directory"
ls