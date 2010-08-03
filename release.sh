#!/bin/sh
#

set -e

BASE=`dirname $0`
cd $BASE
pwd
BASE=`basename $PWD`
PAGES=$BASE-gh-pages

git status

echo releasing $BASE
(cd src && ant jar) \
    && cp lib/$BASE.jar ../$PAGES/ \
    && md5sum lib/$BASE.jar ../$PAGES/$BASE.jar \
    && (cd ../$PAGES && git commit $BASE.jar -m "new release" \
    && git status && git push)

# release.sh