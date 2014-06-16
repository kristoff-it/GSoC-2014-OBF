#!/usr/bin/env python

from __future__ import print_function
import os, sys, gzip, itertools, re, time, datetime
from vcf_miniparser import parse_vcf_together
try:
	import rethinkdb as r
except:
	print('Unable to import the RethinkDB python module.')
	print('To install: pip install rethinkdb')
	print('(You might also want to consider installing the native C++ ProtocolBuffers compiler for better performance)')
	print('\n')
	raise ImportError

# TODO: enable chunked import for append operations
# TODO: allow concurrent --append operations
# TODO: fix edge case for quick imports
# TODO: add --ignore-bad-info and --drop-bad-records switches
# TODO: refined exceptions

def main():
	import argparse

	if r.protobuf_implementation != 'cpp':
		print('# Info: you might want to install the native C++ ProtocolBuffers compiler for better performance.')
		print('# For more information: http://www.rethinkdb.com/docs/driver-performance/')

	parser = argparse.ArgumentParser(description='Load some VCF files.')

	parser.add_argument('--host', default='localhost', 
		help='Host address where the RethinkDB instance is running. Defaults to `localhost`.')

	parser.add_argument('--port', default=28015, type=int,
		help='Port number where the RethinkDB instance is listening. Defaults to `28015`.')

	parser.add_argument('--db', default='VCF', 
		help='Database name where the VCF data is stored. Defaults to `VCF`.')

	parser.add_argument('collection', 
		help='The name of the collection where the VCF files should be stored.')

	parser.add_argument('vcf_filenames', metavar='file', nargs='+',
		help='A VCF file.')

	parser.add_argument('--append', action='store_true',
		help='Add the samples to a collection that might already have items inside (is slower than adding items to a new collection).')

	parser.add_argument('--hide-loading', action='store_true',
		help='Disables showing of loading percentage completion, useful to remove clutter when logging stdout.')

	parser.add_argument('--chunk-size', default=20, type=int,
		help='Select how many records to insert into RethinkDB at a time when doing the initial import. Higher values might improve speed at the expense of memory usage. Defaults to 20.')
	
	parser.add_argument('--hard-durability', action='store_true',
		help='When specified, the database waits for the data to be flushed to disk before aknowledging the operation. Makes the import operations much slower but safer and ensures low memory usage.')
	
	parser.add_argument('--ignore-bad-info', action='store_true',
		help='When specified, info fields that fail to respect their field definition (for example by having a string value inside an `Integer` field) are dropped with a warning. Other INFO fields from the same record are preserved if well formed.')

	args = parser.parse_args()

	# Input sanity is delegated to the import functions.
	# This way you can directly import those and do imports
	# programatically.

	# Connect to RethinkDB
	db_connection = r.connect(host=args.host, port=args.port)

	### Check DB state and init if necessary ###
	check_and_init_db(db_connection, args.db)
	############################################

	# Perform the import:
	start_time = time.time()
	try:
		if not args.append:
			quick_load(db_connection, args.collection, args.vcf_filenames, 
						hide_loading=args.hide_loading, 
						chunk_size=args.chunk_size, 
						hard_durability=args.hard_durability,
						ignore_bad_info=args.ignore_bad_info)
		else:
			append_load(db_connection, args.collection, args.vcf_filenames, 
						hide_loading=args.hide_loading, 
						chunk_size=args.chunk_size, 
						hard_durability=args.hard_durability,
						ignore_bad_info=args.ignore_bad_info)
	except ValueError: 
		# Hide the exception stacktrace from command line output. 
		# In the case of ValueError explanations have already 
		# been printed to stdout.
		exit(1)
	stop_time = time.time()

	print('Loaded all records in', int(stop_time - start_time), 'seconds.')



def check_parameters(collection, vcf_filenames, chunk_size):
	assert re.match(r'^[a-zA-Z0-9_]+$', collection) is not None, \
		"You can only use alphanumeric characters and underscores for the collection name, aborting."
	assert not collection.startswith('__'), \
		"Names starting with double underscores (__) are reserved for internal usage, aborting."
	assert len(vcf_filenames) == len(set(vcf_filenames)), \
		"You are trying to import the same VCF file twice, aborting."
	assert chunk_size > 0,\
		"Invalid value for --chunk-size."



def quick_load(db, collection, vcf_filenames, hide_loading=False, chunk_size=20, hard_durability=False, ignore_bad_info=False):
	"""Performs the loading operations for a new collection."""

	# Check parameters:
	check_parameters(collection, vcf_filenames, chunk_size)

	# Prepare the `durability` parameter for db queries:
	durability = 'hard' if hard_durability else 'soft'

	### CONSISTENCY CHECKS ###
	metadata = r.table('__METADATA__').get(collection).run(db)
	table_list = r.table_list().run(db)
	assert metadata is None and collection not in table_list, \
		"This collection already exists but you didn't specify the `--append` flag, aborting."
	##########################

	# Load parsers:
	headers, samples, parsers, filestreams = init_parsers(vcf_filenames, ignore_bad_info=ignore_bad_info)
	# I want the original filestreams, not the 'fake' ones offered by gzip
	filestreams = [f.fileobj if f.name.endswith('.gz') else f for f in filestreams]

	# Get filesize for every stream, used to print completion percentage and speed.
	total_filesize = float(sum([os.path.getsize(vcf) for vcf in vcf_filenames]))
	total_filesize_as_percentage = total_filesize/100

	## STORE METADATA ##
	collection_info = {
		'id': collection,
		'vcfs': {vcf_filenames[i] : headers[i]._asdict() for i in range(len(headers))},
		'samples': {sample: vcf_filenames[i] for i in range(len(headers)) for sample in samples[i]},
		'doing_init': True
	}

	r.table('__METADATA__').insert(collection_info).run(db)

	# Create the new table required to store the collection:
	r.table_create(collection).run(db)

	
	## STORE ROWS ##
	
	# Timers for completion percentage:
	last_iter = start_time = time.time()

	chunk = list(merge_records(multirecord, vcf_filenames, samples) for multirecord in itertools.islice(parsers, chunk_size))
	while chunk:
		r.table(collection).insert(chunk, durability=durability).run(db)
		chunk = list(merge_records(multirecord, vcf_filenames, samples) for multirecord in itertools.islice(parsers, chunk_size))

		if not hide_loading:
			pos = sum([f.tell() for f in filestreams])
			print('\rLoading: {0:.2f}%'.format(pos/total_filesize_as_percentage), end=' ')
			now = time.time()
			print('@ {} records/second'.format(int(chunk_size/(now-last_iter))), end=' ')
			print('- ETA: {}'.format(datetime.timedelta(seconds=int((now - start_time) * (total_filesize - pos) / pos))), end=' ')
			sys.stdout.flush()
			last_iter = now


	print('\nCompleted loading, waiting for all inserts to be flushed to disk.') 

	# flag insert job as complete once data is written to disk
	r.table(collection).sync().run(db)
	print('OK, updating metadata.')
	r.table('__METADATA__').get(collection).replace(lambda x: x.without('doing_init')).run(db)
	

def append_load(db, collection, vcf_filenames, hide_loading=False, chunk_size=20, hard_durability=False, ignore_bad_info=False):
	"""Performs the loading operations for a collection that already contains samples."""
	
	# Check parameters:
	check_parameters(collection, vcf_filenames, chunk_size)

	# Prepare the parameter for db queries:
	durability = 'hard' if hard_durability else 'soft'

	### CONSISTENCY CHECKS ###
	metadata = r.table('__METADATA__').get(collection).run(db)
	table_list = r.table_list().run(db)

	assert (collection in table_list) == (metadata is not None), \
		"This collection is in a spurious state. Use vcf_admin.py to perform sanity checks."

	if metadata is None:
		print('This is a new collection, switching to direct loading method.')
		return quick_load(db, collection, vcf_filenames, hide_loading=hide_loading, chunk_size=chunk_size, hard_durability=hard_durability, ignore_bad_info=ignore_bad_info)
	else:
		# must check if the collection has finished its pending operations
		assert not metadata.get('doing_init') and not metadata.get('appending_filenames'), \
			"This collection either has still to complete another import operation or has been left in an inconsistent state, aborting. Use vcf_admin to perform consistency checks."
	#########################

	# Load parsers:
	headers, samples, parsers, filestreams = init_parsers(vcf_filenames, ignore_bad_info=ignore_bad_info)
	# I want the original filestreams, not the 'fake' ones offered by gzip
	filestreams = [f.fileobj if f.name.endswith('.gz') else f for f in filestreams]


	# check if there are collisions between new samples and the samples already loaded
	new_samples = set([sample for sublist in samples for sample in sublist])
	old_samples = set(metadata['samples'].keys())
	inter_samples = new_samples & old_samples
	if inter_samples:
		print('Some sample names are colliding, aborting.')
		print('Offending names:', ', '.join(inter_samples))
		raise ValueError

	# check if there are collisions between VCF filenames
	old_vcf_filenames = set(metadata['vcfs'].keys())
	inter_vcf_filenames = set(vcf_filenames) & old_vcf_filenames
	if inter_vcf_filenames:
		print('Some VCF filenames are colliding, aborting.')
		print('Offending names:', ', '.join(inter_vcf_filenames))
		print('Tip: you might consider using a more complete pathname to differentiate between files with the same name, eg:')
		print('$ ./vcf_import.py mycollection mytastycows/samples.vcf mytastiercows/samples.vcf')
		raise ValueError


	# Get filesize for every stream, used to print completion percentage and speed.
	total_filesize = float(sum([os.path.getsize(vcf) for vcf in vcf_filenames]))
	total_filesize_as_percentage = total_filesize/100


	## UPDATE METADATA ##
	collection_info = {
		'vcfs': {vcf_filenames[i] : headers[i]._asdict() for i in range(len(headers))},
		'samples': {sample: vcf_filenames[i] for i in range(len(headers)) for sample in samples[i]},
		'appending_filenames': vcf_filenames
	}

	r.table('__METADATA__').get(collection).update(r.row.merge(collection_info)).run(db)

	## UPDATE ROWS ##
	# Timers for completion percentage:
	last_iter = start_time = time.time()

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
			print("\nFound mismatched REF for CHROM: {} POS: {} when confronting with data already in the database, aborting. All samples in the same collection must share the same reference genome.".format(merged_record['CHROM'], merged_record['POS']))
			raise ValueError

		if not hide_loading:
			pos = sum([f.tell() for f in filestreams])
			print('\rLoading: {0:.2f}%'.format(pos/total_filesize_as_percentage), end=' ')
			now = time.time()
			print('@ {} records/second'.format(int(1/(now-last_iter))), end=' ')
			print('- ETA: {}'.format(datetime.timedelta(seconds=int((now - start_time) * (total_filesize - pos) / pos))), end=' ')
			sys.stdout.flush()
			last_iter = now


	print('\nCompleted loading, waiting for all inserts to be flushed to disk.') 

	# flag insert job as complete once data is written to disk
	r.table(collection).sync().run(db)
	print('OK, updating metadata.')
	r.table('__METADATA__').get(collection).replace(lambda x: x.without('appending_filenames')).run(db)


def check_and_init_db(db_connection, db_name):
	"""Checks if the db exists and has a consistent state. 
	If the db doesn't exits, creates it and performs the init operations."""

	assert re.match(r'^[a-zA-Z0-9_]+$', db_name) is not None, \
		"You can only use alphanumeric characters and underscores for the database name, aborting."

	if db_name == 'VCF':
		print('Defaulting to `VCF` database.')

	# Database exists?
	if db_name not in r.db_list().run(db_connection):
		print('Database does not exist, attempting to create it.')
		r.db_create(db_name).run(db_connection)

		# Db created!
		db_connection.use(db_name)
		try:
			r.table_create('__METADATA__').run(db_connection)
			r.table('__METADATA__').insert({'id': '__METADATA__', 'application': 'vcfthink'}).run(db_connection)
		except r.RqlRuntimeError as e:
			print('Error while doing init operations on the database, aborting.')
			raise e

		print('Database correctly initialized!')

	else:
		db_connection.use(db_name)

	# Does this database belong to this application?
	assert '__METADATA__' in r.table_list().run(db_connection), \
		"The database named `{}` does not belong to this application. Use vcf_init.py to initialize a new database.".format(db_name)
	
	metadata = r.table('__METADATA__').get('__METADATA__').run(db_connection)

	assert metadata is not None and metadata.get('application') == 'vcfthink', \
		"The database named `{}` does not belong to this application. Use vcf_init.py to initialize a new database.".format(db_name)


def init_parsers(vcf_filenames, ignore_bad_info=False):
	"""Opens the filestreams and instantiates each corresponding parser."""

	filestreams = []
	for filename in vcf_filenames:
		if filename.endswith(".gz"):
			filestreams.append(gzip.open(filename, 'r'))
		else:
			filestreams.append(open(filename, 'r'))
	headers, samples, parsers = parse_vcf_together(filestreams, ignore_bad_info=ignore_bad_info)
	
	flattened_samples = tuple([sample for sublist in samples for sample in sublist])
	assert len(flattened_samples) == len(set(flattened_samples)), \
		"Some sample names are colliding. Check your VCF files, aborting."

	return headers, samples, parsers, filestreams



def merge_records(multirecord, vcf_filenames, sample_names):
	"""Performs the merging operations required to store multiple (corresponding) 
	rows of different VCF files as a single object/document into the DBMS.
	Basically it's the glue between parsers and DB."""

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
				sample['GT'] = list(x if x in "|/." else record.ALT[int(x)] for x in alleles)
			
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




if __name__ == '__main__':
	main()
