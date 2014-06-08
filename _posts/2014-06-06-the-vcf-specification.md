---
layout: post_page
title: The VCF specification
---

I'm going to write some of the small quirks that came up while reading the official specificiation, which can be found at [[samtools/hts-specs]](https://github.com/samtools/hts-specs).

While I am no format specification expert, the VCF specification seems to me an agreeable compromise between (human-)readability, flexibility and conciseness. Readability is appreciated in all text formats and, as I've been reminded by multiple people, flexibility is possibly the most important characteristic for a format specification used in bioinformatics. 
Conciseness, as in deduplication, is fundamental when you have to deal with a lot of data with shared structure (which by itself is not that hard, but when you start to take into account custom-typed fields and whatnot, the right thing to do becomes less obvious).

*Considering these 3 aspects, it's no wonder Linus describes XML as ['probably the worst format ever designed'](https://plus.google.com/+LinusTorvalds/posts/X2XVf9Q7MfV).*



### Custom-typed optional custom fields ###
As just mentioned, the most useful feature offered by the format is the ability
to add custom annotations to its contents. Every record can have custom `INFO` fields that relay information about the whole variant. If you want to add custom information about a single call (in case of a VCF file with multiple samples), you can add those as custom `FORMAT` fields which are *multiplexed* for each sample. Every field can also have custom arity (meaning it can be a single value or a list of fixed or unspecified length) and custom type (Flag, Integer, Float, Character or String).

This is cool but, as stated by the official specification, while you should specify those custom fields in the headers, you are not required to. The result is that is possible to get a VCF file with custom fields that lack a proper definition. While it would be possible to infer both the arity and type of those fields, in my specific case (I'm pouring my VCF file inside a DB) this would be a possibly an awkward thing to do, especially considering the 
possibility of a malformed field that would trigger an upcast (for example a field is of Integer type but a record has a typo in it that causes the parser to reconsider all the previous numeric casts as String (since a field must be uniformely typed)) which is an heuristic that in my opinion doesn't seem that useful and, more importantly, prone to errors that might get ignored, even if properly notified. For this reason, at least at this time, I believe the most correct option for undefined custom fields would be to default to a safe cast: custom-length list of Strings.


### #CHROM ###
Although most of the time it contains numerical values, the `#CHROM` field should be considered of String type. It becomes obvious once you have to deal with samples for which there is not a complete sequenced genome avalilable but only scaffolds (*pieces* of the original genome for which the 'global' ordering is yet to be defined). In this case the field contains the name of the corresponding scaffold where the variant is located.

This by itself is not that much surprising but it has a small weird behaviour when combined with ordering: since variants must be ordered, the result is that locally (meaning inside the same chromosome/scaffold) the ordering is numerical, while chromosomes/scaffolds are ordered lexicographically.

The reason I care so much about ordering is because of a pretty important speed up applied when loading multiple VCF files into the DB, which I will cover in the next post.
