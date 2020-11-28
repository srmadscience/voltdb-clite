#!/bin/sh

. $HOME/.profile

cd
cd voltdb-c4d/ddl

sqlcmd --servers=vdb1 < create_db.sql

java -jar $HOME/bin/addtodeploymentdotxml.jar vdb1,vdb2,vdb3 deployment $HOME/voltdb-c4d/scripts/export_and_import.xml
