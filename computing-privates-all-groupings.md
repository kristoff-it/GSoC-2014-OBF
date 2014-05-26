Sample and Group Privates
=========================

This document aims to explain my solution
to the problem of precomputing/indexing
private characteristics of a sequence.
The example proposed will be a slightly 
simplified version of VCF cross-filtering
to find private mutations, but the idea is 
more general and I will briefly explore some
features that might not be of much use for this
particular case.

The result will be an algorithm (for which a Python 
prototype is given) that, after a pretty fast 
precomputation phase, allows fast querying (constant time 
for the most common case) for privates over
any possible grouping of the samples.

Code examples are in python-ish pseudocode, meaning that,
if you care to implement some helper methods I made up to make
the explanation more concise, the code will actually run.

Definition
----------

Fist of all let's make sure we share the same definition of what privates are.


***Given A and B two groups of samples, the privates of A against B are all the positions that satisfy the following condition: All members of group A share the same mutation and it is unique to that group, meaning that it is not present in any member of group B.***


To have an example, consider the following situation:

*(to reduce visual clutter I will use only a single base instead of the whole genotype, as you will soon see it makes no difference to the algorithm)*

| POSs | sA | sB | sC | sD |
|:-----|:---|:---|:---|:---|
| *p1* | T  | .  | T  | A  |
| *p2* | C  | C  | C  | C  |
| *p3* | G  | G  | A  | T  |
| *p4* | .  | A  | A  | A  |
| *p5* | A  | C  | A  | C  |
| *p6* | .  | G  | C  | T  |

This is basically what you can find in a VCF file.
The samples are `{sA, sB, sC, sD}` and the `.` indicates that for that sample the reference base was found.

To make the explanation simpler, I will now focus on the case where the B group is the set complement of A: `privates(A)` implicitly means `privates(A, Universe\A)`.

In our case `Universe` is `{sA, sB, sC, sD}`.

Later I will extend the solution to the case where some samples are left out.

Now, some examples:
```
privates({sA, sB, sC, sD}) = [p2]
privates({sA, sC}) =         [p1, p5]
privates({sB, sC, sD}) =     [p4]
privates({sD}) =             [p1, p3, p6]
```



Naive algorithm
---------------

Now that we agree on what privates are, let's see what the naive implementation looks like, after that I will dissect the problem and show what operations can be decoupled and possibly skipped.

```python
Samples = {...}
# in this example samples and positions will be accessed in this order:
# group[sample].call_at(position)

def privates(groupA):
	groupB = Samples - groupA
	result = []
	for position in POSs:
		gt = groupA[0].call_at(position) # returns the genotype bases
		# get a genotype from a sample to run the equality tests over
		
		gt_is_shared_by_gA = all([sX_gt == gt for sX_gt in groupA.calls_at(position)])
		# all() is a fold over 'and': all([True, True, False]) => False
		
		gt_is_unique_to_gA = not any([sX_gt == gt for sX_gt in groupB.calls_at(position)])
		# any() is a fold over 'or': any([True, True, False]) => True
		
		if gt_is_shared_by_gA and gt_is_unique_to_gA:
			result.append(position)
	
	return result
```
There are lots of things that might need fixing (for example if `all` and `any` were lazy we might save some time) but for now let's leave it at that because the most important speedups are algebraic and come into play once we better define our goals.

Also, this example would have worked even if `groupB` wasn't the complement of `groupA` but I wrote the example this way to be consistent with my premise, once we start building the complete solution you will see why I made such restriction.




Objective
---------
If you want to know the privates for a particular group the only possible approach is to scan the whole VCF file once and get the result, which is what VCFTools already does (at least that what looks like it's doing in `vcf-contrast.pl@120`, damn perl).

What would be nice is to be able to do some precomputation and be able to speed up the process in case we might want to use different groupings.

If you read my proposal, please forget about the whole idea of specifying things at import time. 

With the handy confidence that comes from writing explanations after you already have reached the solution, I argue that **our goal should be to index any possible grouping**.



Building the solution
=====================

I'm going to apply a small refactoring and point out a pretty obvious property of privates, in order to be able to focus on the core point while making sure no rules are broken.

### The previous code: ###

```python
Samples = {...}
# in this example samples and positions will be accessed in this order:
# group[sample].call_at(position)

def privates(groupA):
	groupB = Samples - groupA
	result = []
	for position in POSs:
		gt = groupA[0].call_at(position) # returns the genotype bases
		# get a genotype from a sample to run the equality tests over
		
		gt_is_shared_by_gA = all([sX_gt == gt for sX_gt in groupA.calls_at(position)])
		# all() is a fold over 'and': all([True, True, False]) => False
		
		gt_is_unique_to_gA = not any([sX_gt == gt for sX_gt in groupB.calls_at(position)])
		# any() is a fold over 'or': any([True, True, False]) => True
		
		if gt_is_shared_by_gA and gt_is_unique_to_gA:
			result.append(position)
	
	return result
```

### After refactoring: ###

```python

def privates(groupA):
	groupB = Samples - groupA
	result = []
	
	for position in POSs:
		gts_gA = groupA.calls_at(position)
		gts_gB = groupB.calls_at(position)
		# extract the bases to be passed directly to the test function
		
		if slice_private(gts_gA, gts_gB):
			result.append(position)
	
	return result

def slice_private(group_A_genotypes, group_B_genotypes)
	# eg: slice_private(['A','A'], ['C', 'C', 'T', 'G']) => True
	
	gt = group_A_genotypes[0]
	# get a genotype from a sample to run the equality tests over
	
	for gt_to_test in group_A_genotypes:
		if gt_to_test != gt:
			return False
			
	for gt_to_test in group_B_genotypes:
		if gt_to_test == gt:
			return False
	
	return True
```

Basically all I did was extract the test inside the for loop to 
point out:

* (pretty obvious) Every position can be computed independently of the others
  
* (not so obvious) There is a precise correspondence between `slice_private()` and `privates()`, meaning that it's a legitimate approach to think of the computation as a series of calls to a function that returns `True` or `False`


Computing all groupings
-----------------------

Let's start with the usual naive approach:

```python
def all_groupings_privates(Samples):
	result = {} # dict / hash
	
	for subset in power_set(Samples):	
		result[subset] = privates(subset)
		
	return result
```

Well, this is bad. A loop over an exponentially sized sequence (`power_set(Samples)`) and we do multiple runs over the entire VCF file.
If we want to consider this solution feasible, we must address al those points.

Let's first macroexpand `privates()` into the body of `all_groupings_privates()`:

```python
def all_groupings_privates(Samples):
	result = {} # dict / hash
	
	for groupA in power_set(Samples):
		privates = []
		groupB = Samples - subset
		
		for position in POSs:
			gts_gA = groupA.calls_at(position)
			gts_gB = groupB.calls_at(position)
			
			if slice_private(gts_gA, gts_gB):
				privates.append(position)
		result[groupA.names] = privates
	
	return result
```

If we were able to swap the `for` loops, we might at least gain the fact that we don't read the file over and over, but, can we do that? Long story short, yes.


```python
def all_groupings_privates(Samples):
	result = {} # dict / hash
	
	for position in POSs:
		for groupA in power_set(Samples):
			groupB = Samples - subset
			
			gts_gA = groupA.calls_at(position)
			gts_gB = groupB.calls_at(position)
			
			if slice_private(gts_gA, gts_gB):
				result.setdefault(groupA.names, default=[]).append(position)
				# almost the same as result[groupA.names].append(position),
				# but this way you don't get an exception if the key doesn't exist
	
	return result	
```

Now we don't build the solution "by column", but "by row", and we run over the entire file only once.

There is still the small problem that we are iterating over the power set of `Samples`. I will now push all the "every possible subset" business inside a new function and then work some magic there.



```python
def all_groupings_privates(Samples):
	result = {} # dict / hash
	
	for position in POSs:
		gts = Samples.calls_at(position)
		
		for group_with_a_private in all_groupings_slice_private(gts):
			result.setdefault(group_with_a_private, default=[]).append(position)
			# almost the same as result[group_with_a_private].append(position),
			# but this way you don't get an exception if the key doesn't exist	
			
	return result

def all_groupings_slice_private(genotype_list):
	list_of_groups_with_a_private = []
	...
	return list_of_groups_with_a_private	
```

Before I show you the actual implementation of `all_groupings_slice_private()`, I want to clarify its interface and show you the algebraic trick that allows us to jump over (under?) the exponential bound.

`all_groupings_slice_private()` takes a list of genotypes and returns a subset of the power set of `Samples`. What characterises this subset is that it contains all and only the subsets of `Samples` that have a private at the position that's being inspected.

### The elephant in the room ###

Let me remind you the 2 properties that define a private:

A group has a private at a particular position iif:

1. All members of that group share the same genotype
2. None of the other samples has this particular genotype

If you think about it, each private is an equivalence class and computing all privates is the same as asking to compute the quotient set of `Samples/private`

For example, let's say we have this particular slice:

| POSs     | s0 | s1 | s2 | s3 | s4 | s5 | s6 |
|:---------|:---|:---|:---|:---|:---|:---|:---|
| *pN - 1* | ...| ...| ...| ...| ...| ...| ...|
| *pN*     | A  | C  | C  | A  | G  | C  | T  |
| *pN + 1* | ...| ...| ...| ...| ...| ...| ...|

What are all and only the subsets of `Samples` with a private that you can find at `pN`? The answer is `{{s0, s3}, {s1, s2, s5}, {s4}, {s6}}`.

***(Remember, all privates are evaluated against the complement set.)***

So, without further ado, here's the implementation of our new function:

```python
def all_groupings_slice_private(genotype_list):
	list_of_groups_with_a_private = []
	
	# I will assume that each sample is named after its position 
	# in the list, similarly to what happened in the previous example.
	
	equivalence_classes = {} # dict / hash
	for sample_id, gt in enumerate(genotype_list):
		equivalence_classes.setdefault(gt, default=[]).append(sample_id)
		
	# once the quotient set is computed, all members of each class 
	# is added to the result list to be used as a key once we return
	
	for key in equivalence_classes:
		list_of_groups_with_a_private.append(equivalence_classes[key])

	return list_of_groups_with_a_private	
```
That's it. Few lines, not much magic in the end (although there is a funny game between values that become keys and vice versa).



What if `groupB` is not the complement of `groupA`
-------------------------------------

Or, in other words, what if I want to ignore some samples?
*(the case where both groups share one or more samples is pretty obvious)*

Well, let me first say explicitly how to use the result from `all_groupings_privates()` and then I will extend it to the case in question.

`all_groupings_privates()` returns a hashmap in which keys are groups of samples and values are lists of positions. To get the privates for a grouping you simply try to get the corresponding key. If there is no such key it means that this particular grouping has not privates. For example, if we built our index over the example in the first paragraph: `privates[{sA, sB}] => [p3]`.

To get a private while ignoring some samples, you simply ask for all the privates of your `groupA` and then add to it any other private your `groupA` might share with any of the ignored samples. For example, the privates of `{sC, sD}` against `{sB}` (or, equally, ignoring `{sB}`) are found this way: `privates[{sC, sD}] + privates[{sB, sC, sD}] => [] + [p4]`.

So, basically, the solution is built by querying our index and not by changing the building process. This is the case because if you introduce this new rule from the beginning, all the sweet equivalence-relationship business breaks.



Time and space considerations
-----------------------------

All seems fine but what is the actual asymptotic performance?
There are two main phases: building the index and querying it.


### Building the index ###

To build the index you have to iterate over all positions, then you must build the quotient set over each row and finally store it inside your hash.
Building the quotient set requires to insert each value in the appropriate list inside a hashmap.
This is roughly `O(|POSs| * |Samples|)` operations. In reality it should differ a little depending on how your hashmaps are implemented but the takeaway is that it doesn't do much more work than your parser does, making the parsing operations the most probable bottleneck.

**But, how much space does it take?**

You might think that, since we have `2^(|Sample|)` possible groups, we might still have to store huge amounts of data. This is not the case for the same reason we can skip the exponential time: equivalence is a strong bound when the base alphabet is small.

Imagine we have only single-based genotypes, `[A, C, T, G]` in other words, and have over one million samples. What's the biggest amount of subsets `all_groupings_slice_private()` can output? The answer is `4`, no matter how many samples you add. 

Key length is not a problem because these subsets can be represented by a bitmask so `2^(|Sample|)` groups need only `|Sample|` bits.

In the real-world case there are 10 possible genotypes (`AA, AC, AG, AT, CC, CG, CT, GG, GT, TT`) so the worst case scenario is that if you have `10^7` rows, you get `10 * 10^7 => 10^8` keys. This is a very pessimistic bound anyway.

*(if you think about it, you shouldn't expect a sub-exponential algorithm to produce a potentially exponential number of items)*

### Querying the index ###

Let's start with the good news: 

**Querying for the privates of any group against its complement** (so without ignoring any sample) takes `O(1)` operations: you simply ask for it to the hashmap. In case your hashmap is implemented as a btree then it takes logarithmic time but the formal description is still the same: *pretty damn fast*.

Querying for groups while ignoring some samples is **bad**: you have to search for any private your group might have in common with any of the ignored samples. Basically you must check all keys that have this form: `groupA + power_set(IgnoredSamples)` where `+` is meant as set union. This requires `2^(|IgnoredSamples|)` lookups. There are still some mitigation strategies but the takeaway this time is that you should refrain from building ***The Ultimate Master Index of All Things That Ever Went Trough a Sequencer***.



Funny features and funky properties
-----------------------------------

There are lots of features that we could add while maintaining the same performance levels. Some notable mentions: adding a new sample into the index after it has already been built, caching values, support for range queries (without having to move trough a list).

The fact is that at the moment I have yet to understand completely the underlying algebraic structure (for example the keys, being a subset of power_set(Samples), form a lattice and the refinement relationship between subsets has some kind of connection with the independence of each subset's content) so I'll wait till I know more before I spill the beans.



Proof of concept implementation
-------------------------------

Here's an implementation that's very little optimised but very much comprehensible.

```python
from collections import defaultdict


class ExamplePrivate(object):
   """
   Usage:

   Let sA = "ACGT", sB = "ACG", sC = "ATC":

   >>> mypriv = ExamplePrivate(['sA', 'sB', 'sC'])
   >>> mypriv.extend(1, ['A', 'A', 'A'])
   >>> mypriv.extend(2, ['C', 'C', 'T'])
   >>> mypriv.extend(3, ['G', 'G', 'C'])
   >>> mypriv.extend(4, ['T', None, None])
   >>> mypriv.privates(['sC'])
   (2, 3)
   >>> mypriv.privates(['sA', 'sB'])
   (2, 3)

   To get a private while ignoring some samples:

   >>> mypriv.privates(['sA'], ignore=['sB'])
   (2, 3, 4)

   """

   def __init__(self, sample_names):
      print("This is just an implementation example.\nDo not use in production.")
      self._nodes = defaultdict(list)
      self._sample_mapping = {name: i for i, name in enumerate(sample_names)}
      self._size = 0

   @property
   def size(self):
       return self._size

   def extend(self, index, value_list):
      assert len(value_list) == len(self._sample_mapping), \
         "Mismatch between the number of values and the number of samples."
      
      equivalence_classes = defaultdict(set)
      for i, value in enumerate(value_list):
         if value is None:
            continue
         #else
         equivalence_classes[value].add(i)


      for key in equivalence_classes:
         self._nodes[frozenset(equivalence_classes[key])].append(index)
      self._size += 1

   def privates(self, private_group, ignore=None):
      node_name = frozenset([self._sample_mapping[name] for name in private_group])

      if ignore is None:
         return tuple(self._nodes.get(node_name, ()))

      #else

      ignored_ids = frozenset([self._sample_mapping[name] for name in ignore])
      
      return tuple(sorted([pos for key in self._nodes 
      						if not key - node_name - ignored_ids 
      						for pos in self._nodes[key]]))
      # condition: key.difference(node_name, ignored_ids) == {,}
```
