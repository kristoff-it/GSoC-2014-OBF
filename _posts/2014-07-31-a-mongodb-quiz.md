---
layout: post_page
title: A MongoDB quiz
---

So I'm back and I bring to anyone who's reading this blog a funny and somewhat challenging quiz.

Let's say all documents in your collection have a field that contains an array of either string or null values. In my specific case (or ours, if you care about VCF files and are not just reading GSoC blogs for fun) this field is called `FILTERs` and contains the FILTER values of all VCF files present in the collection and these values, when non-null, are either `'.'`, `'PASS'` or an ID that represents which filter the record failed.

I wanted to add a `--apply-filters` option to the private-finding script. Just like its counterpart in VCFTools, this switch limits the search only to records that didn't fail any filter (so they either `'PASS'`-ed all filters or no filters where applied at all (`'.'`/`null`)).

For example:

```ruby
# This record is bad:
record1 = {'FILTERs' => ['PASS', 'PASS', nil, 'q50']}

# This record is ok:
record2 = {'FILTERs' => ['PASS', '.', nil, '.']}
```

#### The quiz ###
The question is, without falling back to the `'$where'` clause, in which you have a complete JavaScript environment, how do you express this filtering constraint as a MongoDB query?

These are the relevant commands that you can use (taken from [[the complete operators list]](http://docs.mongodb.org/manual/reference/operator/query/)):

* [[`$in`]](http://docs.mongodb.org/manual/reference/operator/query/in/#op._S_in)
	If the field holds an array, then the $in operator selects the documents whose field holds an array that contains at least one element that matches a value in the specified array.
* [[`$nin`]](http://docs.mongodb.org/manual/reference/operator/query/nin/#op._S_nin)
	If the field holds an array, then the $nin operator selects the documents whose field holds an array with no element equal to a value in the specified array.
* [[`$ne`]](http://docs.mongodb.org/manual/reference/operator/query/ne/#op._S_ne)
	On arrays `$ne` works like `$nin` but with a single value. Selects documents where the array does not contan the specified value.
* [[`$not`]](http://docs.mongodb.org/manual/reference/operator/query/not/)
	Negates an expression. When used to query arrays it applies the negation to the check that is applied to every item. In other words `{'FILTERs' => {'$not' => 'PASS'}}`  equals `{'FILTERs' => {'$ne' => 'PASS'}}` meaning that are selected only documents where the `FILTERs` array does not contain the `PASS` value.
* [[`$regex`]](http://docs.mongodb.org/manual/reference/operator/query/regex/#op._S_regex)
	On arrays `$regex` works like `$in` with the single difference that the list of "good" values is expressed as a PCRE. Selects documents whose field holds an array that contains at least one element that matches the regular expression.

There are other commands that might seem useful but, unless I overlooked something very important, they are not useful for this kind of filtering (some might seem to be relevant but they can't be combined correctly when operating on arrays, like `$nor`). Actually most of the operators I listed, while surely correlated, are useless for building a correct solution.

To better specify the constraint: **Only documents whose 'FILTERs' array's values are either `'PASS'`, `'.'` or `null`**.

Some broken examples: 

* `{'FILTERs' => {'$in' => ['PASS', '.', nil]}}`
	Doesn't work because `['PASS', 'q1', nil]` would be selected, for example.
* `{'FILTERs' => {'$nin' => {'$not' => ['PASS', '.', nil]}}`
	Seems nice but is not valid syntax, `$not` negates boolean expressions and can't be used like this.
* `{'FILTERs' => {'$not' => {'$nin' => ['PASS', '.', nil]}}`
	This syntax is valid but works only partially: `['q10', 'x', 'y']` would correctly be filtered out but `['PASS', 'q99']` would pass, for example.

In case anyone is wondering why I'm not simply doing something like `{'FILTERs' => {'$nin' => < all filter ids >}}` the reason is that it's inefficient both in terms of querying (testing over an arbitrarily long list vs (short-circuit) testing over a 3-elements long list) and building: you can't trust the headers to contain all filter IDs. By specification, while encouraged to do so, you are not required to specify any FILTER/INFO/FORMAT field in the headers so I'd have to make sure to "catch" all the different IDs while importing the VCF files; and then if someone decides to apply new filters on data inside the DB, the query suddenly breaks for no good reason.

So, I guess the suspense at this point is unbearable, what might a correct answer be?

```ruby
{'FILTERs' => {'$not' => /^(?!(PASS)|(\.))/}}
```

Yes, a friggin negated regex with negative look-aheads. It also works with `null` values because the (undocumented) `$regex` behaviour is that only strings get tested so `null` values can't fail the match.

In my specific case, since I know for sure how many VCF files are inside a collection, I can do something like this:

```ruby
{'$and' =>[
	'FILTERs.0' => {'$in' => ['PASS', '.', nil]},
	'FILTERs.1' => {'$in' => ['PASS', '.', nil]},
	'FILTERs.2' => {'$in' => ['PASS', '.', nil]},
	...
]}
```
Where the dotted notation allows to select a specific index of the array.

But, seriously, in cases where there arrays are of unknown and/or variable length it's a nightmare to get this kind of filtering to work. The whole situation was created because other commands don't let the user specify the 'OR' part of the clause at the right level.

On top of that all these commands are so sugarcoated that corner cases are hard to know unless you stumble upon them, hence, the takeaway:

**Too much syntactic sugar gives you semantic diabetes, be careful.**




