# Script to update HUDI pom version and fix the various drogon files automatically.


# retrieve current pom version
CURRENT_VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`


# increment the version
IFS='.' read -a tokens <<< "$CURRENT_VERSION"
last_index=${#tokens[@]}-1
tokens[$last_index]=`expr ${tokens[$last_index]} + "1"`
NEW_VERSION=$(IFS=. ; echo "${tokens[*]}")


# accept choice
echo ""
while true; do
    read -p "Do you wish to upgrade HUDI from $CURRENT_VERSION to $NEW_VERSION [yNe(dit)]? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        [eE]* ) read -p "Please enter the new HUDI version (current=$CURRENT_VERSION): " NEW_VERSION;;
        * ) exit;;
    esac
done


# Upgrade HUDI version in the various json files for drogon
echo ""
output=`grep 'HUDI_VERSION": ' --files-with-matches drogon/*.json`
IFS=$'\n' read -rd '' -a DROGON_FILES <<< "$output"
search='"HUDI_VERSION": ".*"'
replace='"HUDI_VERSION": "'"$NEW_VERSION"'"'
for file in "${DROGON_FILES[@]}"
do
    echo "Updating HUDI version in drogon file $file"
    sed -i "" "s/$search/$replace/g" $file 
done

echo "Upgrading HUDI pom to version $NEW_VERSION"
mvn versions:set -DgenerateBackupPoms=false -DnewVersion=$NEW_VERSION 1>/dev/null


# show diff if required
echo ""
while true; do
    read -p "Show diff of changes [yN]? " yn
    case $yn in
        [Yy]* ) git diff && break;;
        * ) break;;
    esac
done


# commit and push changes
echo ""
while true; do
    read -p "Commit changes and push to branch [yN]? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) exit;;
    esac
done


# commit changes
git add -u
git commit -m "[UBER] Upgrade version to $NEW_VERSION" 1>/dev/null
echo ""


# push the branch
git push 1>/dev/null
if [ $? -ne 0 ]; then
    echo "Failed to push the branch"
else
    echo "All done. You can publish the new version at https://ci.uberinternal.com/job/publish-to-artifactory/"
fi

