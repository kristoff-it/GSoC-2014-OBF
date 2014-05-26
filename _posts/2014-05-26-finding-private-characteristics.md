---
layout: post_page
title: Finding private SNPs
---

Before I start talking about the interesting bits, the naming problem must be addressed.
Appranently, dfferent people use the term 'private SNP' to mean different things:

* The ISOGG uses the term to indicate SNPs present in only a small part of a population. [1](http://www.snpedia.com/index.php/Private_SNP) [2](http://www.isogg.org/tree/ISOGG_Glossary.html)
* VCFTools' `vcf-subset` command uses the `--private` switch to filter the variants where only the subset columns carry an alternate allele (meaning all the other columns must have the reference allele or, in other words, `0/0` as `GT` value). [3](http://vcftools.sourceforge.net/perl_module.html#vcf-subset)

The project idea mentioned this particoular problem:
*"... the variations found in 100 samples and extract all the mutations that are present in 50 samples but are not present in the other 50 ..."*

After getting some more details about the qustion from my mentors, the resulting definition that should be implemented is a slightly more general:

**"All the variants where all the samples of a defined group share the same allele and none of the remaining samples has it."**

So, from now on I'm going to use this last definition as the meaning of 'private SNPs' (or, more concisely, 'privates'), with the implicit agreement that the same definition could be extended to other kind of mutations.


### Finding privates ###

As of now, to find the privates of a single sample in a multisample VCF file, the most appropriate tool is VCFTools' command `vcf-contrast`: you define your chosen sample by prepending a `+` to its name, the background/control group is defined by a `-` sign and the result is what you expect from the definition.

Unfortunately, altough `vcf-contrast` allows you to specify more than one `+` sample, the interpretation that the tool has of what you want differs from our definition of private. `+`-ing more than one sample, for `vcf-contrast`, means that you want to combine your group with an `OR` clause, resulting in all variants where the excluded samples don't have `ANY` of the alleles present in the `+` group, while instead we need all samples in the `+` group to share the same allele.

This means that, while sample privates are easy to compute, group privates don't have an official tool to be computed and, when in need of such filtering, people resort to cumstom handmade scripts.

Not only that, but if you want to know the privates of each sample inside the same VCF file, you need to launch the tool once for each sample. Surely something better can be done.


### Finding privates for all possible groupings ###

Fortunately, as it turns out, computing the privates for all the possible groupings of samples is not that hard.
The resulting algebric structure is so nice that all that is needed is to compute equivalence classes.
I wrote an explanation of the algorithm which you can find on the main branch of this project's github repository: [computing-privates-all-groupings.md](https://github.com/kappaloris/GSoC-2014-OBF/blob/master/computing-privates-all-groupings.md).











