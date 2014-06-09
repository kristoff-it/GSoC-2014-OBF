#!/usr/bin/env python

from __future__ import print_function
import os, sys, gzip, itertools, re
from vcf_miniparser import parse_vcf_together
import rethinkdb as r


# TODO: --force-append

def main():
	import argparse, time, re

	parser = argparse.ArgumentParser(description='Load some VCF files.')

	parser.add_argument('--host', default='localhost', 
		help='Host address where the RethinkDB instance is running. Defaults to `localhost`.')

	parser.add_argument('--port', default=28015, 
		help='Port number where the RethinkDB instance is listening. Defaults to `28015`.')

	parser.add_argument('--db', default='VCF', 
		help='Database name where the VCF data is stored. Defaults to `VCF`.')

	parser.add_argument('collection', 
		help='The name of the collection where the VCF files should be stored.')

	parser.add_argument('vcf_filenames', metavar='file', nargs='+',
		help='A VCF file.')

	parser.add_argument('--append', action='store_true',
		help='Add the samples to a collection that might already have items inside. (is slower than adding items to a new collection)')

	parser.add_argument('--hide-loading', action='store_true',
		help='Disables showing of loading percentage completion, useful to remove clutter when logging stdout.')


	args = parser.parse_args()

	### Input sanity ###
	assert re.match(r'^[a-zA-Z0-9_]+$', args.db) is not None, \
		"You can only use alphanumeric characters and underscores for the database name, aborting."
	assert re.match(r'^[a-zA-Z0-9_]+$', args.collection) is not None, \
		"You can only use alphanumeric characters and underscores for the collection name, aborting."
	assert not args.collection.startswith('__'), \
		"Names starting with double underscores (__) are reserved for internal usage, aborting."
	assert len(args.vcf_filenames) == len(set(args.vcf_filenames)), \
		"You are trying to import the same VCF file twice, aborting."
	####################

	# Connect to RethinkDB
	db_connection = r.connect(host=args.host, port=args.port)

	### Db state check ###
	check_db_status(db_connection, args.db)
	######################

	### Collection state check ###
	if not args.append:
		metadata = r.table('__METADATA__').get(args.collection).run(db_connection)
		assert args.collection not in r.table_list().run(db_connection) and metadata is None, \
			"This collection already exists but you didn't specify the `--append` flag, aborting."
		#assert metadata.get('doing_init') is None and metadata.get('appending_filenames') is None, \
		#	"There either is another pending job or the last job failed and left the collection in an inconsistent state, aborting. Use vcf-admin to perform sanity checks."
		#	"There is already a pending job on this collection. It might be a legitimate operation " +\
		#	"still running or the collection might have been left in an inconsistent state. Add the flag `--force-append` to perform concurrent imports, otherwise use vcf_admin to perform sanity checks."
	##############################

	# Perform the actual import:
	start_time = time.time()
	if not args.append:
		quick_load(db_connection, args.collection, args.vcf_filenames, hide_loading=args.hide_loading)
	else:
		append_load(db_connection, args.collection, args.vcf_filenames, hide_loading=args.hide_loading)
	stop_time = time.time()

	print('Loaded all records in', stop_time - start_time, 'seconds.')





def merge_records(multirecord, vcf_filenames, sample_names):
	"""Performs the merging operations required to store multiple (corresponding) 
	rows of different VCF files as a single object/document into the DBMS."""

	assert all(multirecord[0][1].REF == record.REF for _, record in multirecord ), \
		"Found mismatched REF for #CHROM: {}, POS: {}, aborting. All samples in the same collection must share the same reference genome.".format(multirecord[0][1].CHROM, multirecord[0][1].POS)

	CHROM = multirecord[0][1].CHROM
	POS = multirecord[0][1].POS
	IDs = {}
	REF = multirecord[0][1].REF
	QUALs = {}
	FILTERs = {}
	INFOs = {}
	samples = {}

	for i, record in multirecord:
		record.ALT.insert(0, record.REF)
		for sample in record.samples:
			if 'GT' in sample:
				alleles = re.split(r'([|/])', sample['GT'])
				sample['GT'] = [x if x in "|/." else record.ALT[int(x)] for x in alleles]
			
		IDs[vcf_filenames[i]] = record.ID
		QUALs[vcf_filenames[i]] = record.QUAL
		FILTERs[vcf_filenames[i]] = record.FILTER
		INFOs[vcf_filenames[i]] = record.INFO
		samples.update([(sample_names[i][k], sample_data) for (k, sample_data) in enumerate(record.samples)])

	return {
		'id': '-'.join([CHROM, str(POS)]),
		'CHROM': CHROM,
		'POS': POS,
		'IDs': IDs,
		'REF': REF,
		'QUALs': QUALs,
		'FILTERs': FILTERs,
		'INFOs': INFOs,
		'samples': samples
	}



def init_parsers(vcf_filenames):
	"""Opens the filestreams and instantiates each corresponding parser."""

	filestreams = []
	for filename in vcf_filenames:
		if filename.endswith(".gz"):
			filestreams.append(gzip.open(filename, 'r'))
		else:
			filestreams.append(open(filename, 'r'))
	headers, samples, parsers = parse_vcf_together(filestreams)
	
	flattened_samples = tuple([sample for sublist in samples for sample in sublist])
	assert len(flattened_samples) == len(set(flattened_samples)), \
		"Some sample names are colliding. Check your VCF files, aborting."

	return headers, samples, parsers, filestreams



def quick_load(db, collection, vcf_filenames, hide_loading=False):
	"""Performs the loading operations for a new collection."""

	headers, samples, parsers, filestreams = init_parsers(vcf_filenames)

	# get filesize for every stream, to print completion percentage.
	total_filesize = sum([os.path.getsize(vcf) for vcf in vcf_filenames])/100.0

	## STORE METADATA ##
	collection_info = {
		'id': collection,
		'vcfs': {vcf_filenames[i] : headers[i]._asdict() for i in range(len(headers))},
		'samples': {sample: vcf_filenames[i] for i in range(len(headers)) for sample in samples[i]},
		'doing_init': True
	}

	r.table('__METADATA__').insert(collection_info).run(db)

	## STORE ROWS ##
	r.table_create(collection).run(db)


	for multirecord in parsers:
		r.table(collection).insert(merge_records(multirecord, vcf_filenames, samples), durability='soft').run(db)
		if not hide_loading:
			print('\rLoading: {0:.2f}%'.format(sum([f.fileobj.tell() if f.name.endswith('.gz') else f.tell() for f in filestreams])/total_filesize), end='')
			sys.stdout.flush()

	# flag insert job as complete once data is written to disk
	r.table(collection).sync().run(db)
	r.table('__METADATA__').get(collection).replace(lambda x: x.without('doing_init')).run(db)
	print(' - done.')
	


def append_load(db, collection, vcf_filenames, hide_loading=False):
	"""Performs the loading operations for a collection that already contains samples."""

	# check for inconsistent db state
	table_exists = collection in r.table_list().run(db)
	meta_exists = r.table('__METADATA__').get(collection).run(db) is not None

	assert table_exists == meta_exists, \
		"This collection is in an inconsistent state. Use vcf_admin to perform sanity checks."

	if not meta_exists:
		print('This is a new collection, switching to direct loading method.')
		return quick_load(db, collection, vcf_filenames, hide_loading=hide_loading)
	else:
		# must check if the collection has finished its init operations
		assert not r.table('__METADATA__').get(collection).has_fields('doing_init').run(db) \
			   and not r.table('__METADATA__').get(collection).has_fields('appending_filenames').run(db), \
			"This collection either has still to complete another import operation or has been left in an inconsistent state, aborting. Use vcf_admin to perform consistency checks. "

	headers, samples, parsers, filestreams = init_parsers(vcf_filenames)

	# get filesize for every stream, to print completion percentage.
	total_filesize = sum([os.path.getsize(vcf) for vcf in vcf_filenames])/100.0

	# check if there are collisions between new samples and the samples already loaded
	new_samples = set([sample for sublist in samples for sample in sublist])
	old_samples = set(r.table('__METADATA__').get(collection)['samples'].keys().run(db))
	inter_samples = new_samples & old_samples
	if inter_samples:
		print('Some sample names are colliding, aborting.')
		print('Offending names:', ', '.join(inter_samples))
		exit(1)

	# check if there are collisions between VCF filenames
	old_vcf_filenames = set(r.table('__METADATA__').get(collection)['vcfs'].keys().run(db))
	inter_vcf_filenames = set(vcf_filenames) & old_vcf_filenames
	if inter_vcf_filenames:
		print('Some VCF filenames are colliding, aborting.')
		print('Offending names:', ', '.join(inter_vcf_filenames))
		print('Tip: you might consider using a more complete pathname to differentiate between files with the same name, eg:')
		print('$ vcf_import.py mycollection mytastycows/samples.vcf mytastiercows/samples.vcf')
		exit(1)

	## UPDATE METADATA ##
	collection_info = {
		'vcfs': {vcf_filenames[i] : headers[i]._asdict() for i in range(len(headers))},
		'samples': {sample: vcf_filenames[i] for i in range(len(headers)) for sample in samples[i]},
		'appending_filenames': vcf_filenames
	}

	r.table('__METADATA__').get(collection).update(r.row.merge(collection_info)).run(db)


	## UPDATE ROWS ##
	for multirecord in parsers:
		merged_record = merge_records(multirecord, vcf_filenames, samples)

		result = r.table(collection).get(merged_record['id']).replace(
			r.branch(r.row == None, 
				merged_record, # new record
				r.branch(r.row['REF'] == merged_record['REF'],
					r.row.merge(merged_record),
					r.error())), durability='soft').run(db)
		
		if result['errors']:
			print("\nFound mismatched REF for #CHROM: {}, POS: {} when confronting with data already in the database, aborting. All samples in the same collection must share the same reference genome.".format(merged_record['CHROM'], merged_record['POS']))
			exit(1)

		if not hide_loading:
			print('\rLoading: {0:.2f}%'.format(sum([f.fileobj.tell() if f.name.endswith('.gz') else f.tell() for f in filestreams])/total_filesize), end='')
			sys.stdout.flush()

	# flag the append job as complete once data is written to disk
	r.table(collection).sync().run(db)
	r.table('__METADATA__').get(collection).replace(lambda x: x.without('appending_filenames')).run(db)

	print(' - done.')



def check_db_status(db_connection, db_name):
	"""Checks if the db exists and has a consistent state. 
	If the db doesn't exits, creates it."""

	if db_name == 'VCF':
		print('Defaulting to `VCF` database.')

	# Database exists?
	if db_name not in r.db_list().run(db_connection):
		print('Database does not exist, attempting to create it.')
		try:
			result = r.db_create(db_name).run(db_connection)
		except r.RqlRuntimeError:
			print('Unable to create the database, aborting.')
			exit(1)

		# Db created!
		db_connection.use(db_name)
		try:
			r.table_create('__METADATA__').run(db_connection)
			r.table('__METADATA__').insert({'id': '__METADATA__', 'application': 'vcfthink'}).run(db_connection)
		except:
			print('Error while inserting metadata inside the new database, aborting.')
			exit(1)

		print('Database correctly initialized!')

	else:
		db_connection.use(db_name)

	# Does this database belong to this application?
	assert '__METADATA__' in r.table_list().run(db_connection), \
		"The database named `{}` does not belong to this application. Use vcf_init.py to initialize a new database.".format(db_name)
	
	metadata = r.table('__METADATA__').get('__METADATA__').run(db_connection)

	assert metadata is not None and metadata.get('application') == 'vcfthink', \
		"The database named `{}` does not belong to this application. Use vcf_init.py to initialize a new database.".format(db_name)

if __name__ == '__main__':
	main()
