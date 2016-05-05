File Indexer

This is a set of scripts, currently under heavy development
to help me sort out my files repository (photos, documents, scans, code, etc)



dirlisting.rb, uses a mysql connection to 
record the size, the modification time and the sha256 for each file under a tree.

Originally I was using the external program sha256sum which was fast, but I had
problems I couldn't solve with the encoding of the filenames and the execution of the program.

So I turned to Digest::sha256 which is slower but works ok.

That is why now I turning the code to having multiple threads.

The database layout is :

Table : files
sha256(primary key) | fullpath | size | modification time (float number)

Any conflicting primary key is checked against File.indentical? and if so, it is
ignored, otherwise it is logged to a table for duplicate_files:

Table: duplicate_files
sha256(foreing key) | fullpath


So in the end I have two tables, the second one being files with the same content as 
a file in the first one.

The next step is to have another script, create hard links for all the files of the
second table.


As you may guess, this code is under heavy development and I wouldn't recommend anyone
to execute it as is.
