File Indexer

This Ruby script, uses a mysql connection to 
record the size, the modification time and the sha256 for each file under a tree.

The database layout is :

Table : files
sha256(primary key) | fullpath | size | modification time

Any conflicting primary key is checked against File.indentical? and if so, it is
ignored, otherwise it is logged to table duplicate_files:

Table: duplicate_files
sha256(foreing key) | fullpath

