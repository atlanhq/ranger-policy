echo "Maven Building"
mvn -DskipJSTests -DskipTests=true -Drat.skip=true clean package -Pranger-admin

echo "[DEBUG] listing distro/target"
ls distro/target