---
layout: post_page
title: What is a VCF (Variant Call Format) file?
---

VCF files contain the interesting parts of the genetic sequence of one or more samples. To understand what's of interest and what isn't, there's a good deal of precomputation and filtering required, making VCF files the result of a not-so-short processing chain.


### How VCF files are created ###

To properly understand how these files get created, we must start from the beginning of the processing chain. 

First of all, some genetic material from a sample is prepared and is placed using a pipette on a plate that is inserted into a machine that is capable of reading the actual base sequence. Usually these machines allow more than one sample to be placed in.

The machine, unfortunately, is not capable of producing a single, correct and complete read and call it a day. What the machine does is perform a lot of overlapping reads of parts of the original sequence and try to generate enough data for you to be able to infer the original (correct and complete) sequence.

Depending on which specific technology the machine uses:

   * It might yield a higher or lower number of reads in a specific amount of time.

   * The single reads might be longer or shorter, usually this is proportionally inverse of how fast the machine is. 

   * Certain regions of the original sequence might be over or under read, resulting in various problems (what if a certain region is completely absent from the reads?).

On top of that, the reads might contain errors (an `A` might have been misread as a `G`, for example).

#### FASTQ ####

After a few hours or days, depending on how the machine was configured, what you get back is a FASTQ file which contains all the reads performed by the machine.
Each read is also coupled with an equally long string of quality evalutaions for every single base: this is how the machine tells you how certain it is that a particular base has been recognized correctly.

So, at the moment we have a lot of partial reads and want to "stitch" them back to the original form.

There are two possibilities:

[1] You try to do the stitching without any external information.

[2] Someone already did [1] for another sample that is similar enough to yours to use as a reference.

*Both cases require you to reassembly a jigsaw puzzle. 
While in [2] you have a reference that helps you understand what-shoud-go-where, in [1] you don't even know what the subject is.* 

For simplicity, let's say our case is [2].

#### SAM ####

Providing your FASTQ file and a reference genome to a kind of program called *read aligner*, you get a SAM file  that tells where each of your reads should be aligned.

More specifically, for each read, you get the coordinate where its counterpart starts in the reference genome plus other informations like how certain an alignment is.
As already pointed out, reads are not perfect, the shorter ones are tricky and, on top of that, the two sequeneces - yours and the reference one - have some actually meaningful differences that add more difficulty to the task.

#### VCF ####

VCF files are created by programs called *variant callers* and basically they are the result of filtering out matching bases from SAM files. What remains are the "interesting" bases: how our sample differs from the reference one. Since most bases are supposed to be shared (unless you're using a really distant reference, like using a Vulcanian genome as a reference for your Andorian sample, for example) the resulting file will be much smaller and, in practical terms, more useful.


### What are VCF files used for ###

There are lots of possible applications, the ones I know about are:

* You are trying to identify how some samples differ from others to be able to distinguish between different groups.

* You want to know if a sample has a particular predisposition. Using information stored in other databases you can annotate each "interesting" position with the information on how it might contribute to a particular predisposition or condition.

* A group of samples expresses a particoular feature that you believe migth have a genetic origin. By matching the "interesting" bases of these samples against a "control group" (similar samples that don't show the feature in question) you can isolate the most probable bases involved.


In my next post I'm going to write in more detail about VCF files, their structure and small gotchas that came up while trying to build a parser.



