---
layout: post_page
title: Completed vcf-import 
---
The import script is now complete.
All kinds of import operations are supported and errors are handled nicely.

Usage is as simple as possible:

```sh
$ jruby vcf-import.rb collectionName file1.vcf file2.vcf ...
``` 

Other switches can be used to tweak buffers' sizes and the number of processing threads for each step.

The script shows the total number of imported records, current import speed and the saturation level of each queue (which is both interesting to look at and informative about possible adjustments that would widen a possible bottleneck). Unfortunately, a completion percentage and/or an ETA timer is not available (for now, at least) because the HTSJDK parser hoards the `InputStream` generated from the VCF file pathname and apparently being able to tell how many seconds/hours/eons will take for a parsing operation to complete [[is not considered a top priority]](https://github.com/samtools/htsjdk/issues/63#issuecomment-49464602). It also apparently is considered an internal feature (???).

Well for now I'll leave it at that and come back to it once the rest is done.
Or maybe I'll be lucky and someone <del>with more masochistic tendencies than me</del> will have already submitted a patch.

I'm very satisfied about how this script came to be.
The only (quite marginal, I believe) performance improvements that might be applied are:

* Group records in chunks to lower any trashing that might derive from lock contention between multiple threads trying to push/pull items from the same queue.
* Add `Thread.pass` statements to try to induce a better thread scheduling rithm.








