---
layout: post_page
title: Loading multiple VCF files
---

As I mentioned in the previous post, when importing multiple VCF files at once, I try to align them in terms of `#CHROM` and `POS` (similarly to what PyVCF's [[walk_together]](http://pyvcf.readthedocs.org/en/latest/FILTERS.html#vcf.utils.walk_together) does). The reason for going trough all that trouble is because of how the datamodel is shaping up.

While I'm not ready to talk about it in full detail, some considerations are already 'locked-in'. For example for each absolute position (= chromosome/position combination) there is a single record that contains the data for all the samples. This has multiple implications (in descending order of importance):

1. The total number of items inside a collection is not strongly correlated with the number of samples (as it would be if we created a different object for each sample).

2. Building and using indexes (this applies to the index needed for privates too!) becomes easier because we can both define 'record-level' indexes and 'sample-level' indexes effortlessly (as long as multi-indexes are supported by the DBMS). I will write more about that later.

3. In the end, when querying, having a 'record' atom is the most natural way to describe what you want. The only difference is that some fields start having multiple values since different VCF files migth have different opinions (for example a record might have passed all filters in a VCF file, while its counterpart in another file might have not).

### Multirecords ###

Having all corresponding vcf-records as a single *db-multirecord* brings up a new step: data must be merged. For this reason, whenever possible, I try to do all the merging beforehand to then result in a direct insert query, otherwise every insert would require first to read what's already in the DB, merge it with the new data and finally update the record. Talk about I/O bound heat death of the universe.

Since this can happen only when you are importing multiple VCF files at once, the problem can be mitigated but not completely avoided: when you want to add some more VCF files in a collection that already has been previously initialized, you still have to do some round trips.

It's not that bad: every insert requires to read from the DB but, since I am still aligning my VCF files, it must be done only once.

For this problem RethinkDB has a wonderful approach that makes the operation very concise to express and saves a *serialize-send-deserialize-compute-serialize-send* trip:

RethinkDB has an interesting approach to querying: queries are expressed in a somewhat LISP-y DSL that gets compiled to JS and executed by the server. This, combined with a pretty awsome set of operations (which in any case is mostly synctactic sugar since you can also write custom JS inside a query) allows the 'append' updates to be performed as a *'send the new data to the DBMS and let it to the merging if necessary'* query.

The code in the end is as follows:

```python
r.table(collection)
   .get(new_data_to_add['id'])
   .replace(r.branch(r.row == None, 
               new_data_to_add, # new record
               r.row.merge(new_data_to_add))).run(db)
```
`r.branch` is the functional equivalent of an if statement and is expressed as `r.branch(condition, do_if_true, do_if_false)`. Basically the query is checking if the record exists in the database (`r.row == None`). If not then we can directly insert the new data, otherwise the DBMS does the merging operations (`r.row.merge(new_data_to_add)`). Pretty neat.

The real code has a slight complication: I want to ensure the reference genomes are the same so another branch is added to fail in case of a mismatch.

```python
r.table(collection)
   .get(new_data_to_add['id'])
   .replace(r.branch(r.row == None, 
               new_data_to_add, # new record
               r.branch(r.row['REF'] == new_data_to_add['REF'],
                  r.row.merge(new_data_to_add),
                  r.error()))).run(db)
```

The fact that this problem gets solved with such elegance (both in terms of soundness of sequence of operations and not too much convoluted code) gives RethinkDB a pretty consistend upper hand against other NoSQL DBMSs for this kind of applications.

