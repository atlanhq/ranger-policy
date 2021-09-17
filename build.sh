echo "Maven Building"
mvn -pl '!plugin-kylin,!ranger-kylin-plugin-shim' -DskipJSTests -DskipTests=true -Drat.skip=true clean package -Pall

echo "[DEBUG] listing distro/target"
ls distro/target

echo "[DEBUG] listing /home/runner/work/ranger-policy/ranger-policy/target"
ls /home/runner/work/ranger-policy/ranger-policy/target