---
layout: post_page
title: Multithreaded vcf-import 
---

I'm almost done writing the import script with multithreading support.
It actually feels weird to be able to have properly parallel concurrency (no GIL? is this real life?)

Anyway, this allows to have a somewhat straigthforward processing pipeline with queues as syncrhonization mechanism.

Sketch of its structure:
```
# P1 P2 PN     # parser threads
#  |  |  | 
# b1 b2 bN     # parser buffers
#   \ | /
#  ALIGNER     # aligner thread
#     |
#     mb       # merger buffer 
#     | 
#   / | \
#  M1 M2 MN    # merger threads
#   \ | /
#     |
#    mdbb      # MongoDB buffer
#     |
#    / \
#  MDB1 MDBN   # MongoDB import threads 
```

To better describe each point:

* Each parser has its own thread and queue (one for each VCF file is being imported).
* A single thread performs alignments (groups records by CHROM:POS) and stores the result into another queue
* Multiple threads perform the merging operations (which seems to be the heaviest part of the job) and store the result into another queue
* Multiple threads perform the insert/update operations of merged records via MongoDB's `bulk_insert`.

This is pretty neat IMO.

I faced two problems tho:

* MongoDB doesn't like dots inside hash keys because there is a dotted notation to get nested elements. This made me unable to store both filenames and sample names as hash keys (the first kind most surely has dots inside and sample names might too, according to the VCF specification) whithout enforcing some kind of weird renaming convention (`file<dot>vcf` for example). As a result metadata regarding filenames and samplenames is stored as lists.

* The HTSJDK parser doesn't offer (at least to my knowledge) a way to get a handle to the `InputSteam` object that represents the actual open file. The `VCFFileReader` (the class that does the actual parsing) constructor wants a `java.io.File` instance which is only a (bloated :) ) pathname. For this reason I'm unable to `tell()` at a given time which position of the VCF file we're reading and consequently I have no way of showing a completion percentage. This seems kind of weird, the stream handle shouldn't be a `super private` property. I've opened an issue [[here]](https://github.com/samtools/htsjdk/issues/63), hopefully someone will answer quickly.  

