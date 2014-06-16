---
layout: post_page
title: Organizing VCF files
---

How should multiple VCF files be stored inside a single DB?

The strucure is implemented as follows:

A database contains multiple collections and their relative metadata (names of the VCF files inside each collection, headers, and so on). A collection contains a variable number of VCF files that share the same reference genome and that otherwise don't need to share any scruture (they might have been annotated differently, for example).

### Import operations ###
I've implemented the import script [[vcf_import.py]](https://github.com/kappaloris/GSoC-2014-OBF/blob/master/vcf_import.py) that, keeping in mind this structure, can be used as follows (you only need to have rethinkdb installed and its python driver (pip install rethinkdb)):

`$ ./vcf_import.py humans file1.vcf.gz file2.vcf.gz file3.vcf ...`

Use the `--help` switch to get more info about the possible parameters.

You can optionally select how many records to upload at a time which might help improve the speed you if you're unable to install ProtocolBuffers' native C++ compiler.

Loading samples into a new collection is acceptably fast (its speed is basically capped by how fast the database can perform imports).

To add samples to an already existing collection use the `--append` flag but beware that for now it's much slower, but, while slower it will always be (as explained in my previous post), I hope to remove the 'much' soon by enabling the same chunked insert support used by the first import method.

### Administration ###
To manage your collections you can use [[vcf_admin.py]](https://github.com/kappaloris/GSoC-2014-OBF/blob/master/vcf_admin.py).

It allows you to perform the usual operations (list, copy, delete) and sanity checks in case of failure. For example, what if an import operation failed because of a malformed VCF file or simply because the user decided to kill the process? `vcf_admin.py` allows you to check for these kind of situations and fixes the problem. In particular if an `--append` import failed you can revert it without losing the entire collection.

Both scripts also have the command line interface and the actual operations separated. If you want you can import the specific functions and perform operations programatically (ofc its all undocumented for now :)).

If you do try it, don't forget to check out RethinkDB's web administration (usually listening on port 8080).

Next stop is writing `vcf_private.py` to finally be able to query for privates!

