# File Indexer

This is a set of scripts, currently under heavy development
to help me sort out my files repository (photos, documents, scans, code, etc)


>registerFiles.rb

is a script that will record the sha256, size and modification time of each file under a tree.

It is _multithreaded_ and uses sequel as the adapter for the database.

The database layout is :

*Table : files*
sha256(primary key) | fullpath | size | modification time (float number)

Any conflicting primary key is checked against File.indentical? and if so, it is
ignored, otherwise it is logged to a table for duplicate_files:

*Table: duplicate_files*
sha256(foreing key) | fullpath

So in the end I have two tables, the second one being files with the same content as 
a file in the first one.

As you may guess, this code is under heavy development and I wouldn't recommend anyone
to execute it as is.
