#!/usr/bin/env python

from __future__ import print_function
import re
try:
	import rethinkdb as r
except:
	print('Unable to import the RethinkDB python module.')
	print('To install: pip install rethinkdb')
	print('(You might also want to consider installing the native C++ ProtocolBuffers compiler for better performance)')
	print('\n')
	raise ImportError

class BadDatabase(Exception):
	pass
class BadCollection(Exception):
	pass


def main():
	import argparse, time, re

	parser = argparse.ArgumentParser(description='Manage the VCF database.')

	parser.add_argument('--host', default='localhost', 
		help='Host address where the RethinkDB instance is running. Defaults to `localhost`.')

	parser.add_argument('--port', default=28015, 
		help='Port number where the RethinkDB instance is listening. Defaults to `28015`.')

	parser.add_argument('--db', default='VCF', 
		help='Database name where the VCF data is stored. Defaults to `VCF`.')

	parser.add_argument('command', choices=['help', 'list', 'check', 'fix', 'rename', 'copy', 'delete'], 
		help='The operation that must be performed.')
	
	parser.add_argument('options', nargs='*',  
		help='Options for the selected command. Use the help command to know more about each command.')
	
	parser.add_argument('-f', action='store_true',
		help='Execute a fix or delete command without requiring a confirmation.')

	

	args = parser.parse_args()

	if args.command == 'help':
		print('Command list:')
		print('help                Show this message and exit.')
		print('')
		print('list [collection]   List all collections or detailed')
		print('                    info pertaining a single collection.')
		print('')
		print('check               Check consistency status of all ')
		print('                    collections.')
		print('')
		print('fix [collection [-f]]    Fix all spurious collections or')
		print('                         a single inconsistent collection.')
		print('                         If `-f` is not specified, ')
		print('                         requires confirmation.')
		print('')
		print('rename old new      Rename a collection.')
		print('')
		print('copy source dest    Create a copy of a collection.')
		print('                    Indexes are not copied.')
		print('')
		print('delete name [-f]    Delete a collection. If `-f` is not')
		print('                    specified, requires confirmation.')
		return

	# from this point onward a db connection is required
	
	# Connect to RethinkDB
	db_connection = r.connect(host=args.host, port=args.port)

	if args.db == 'VCF':
		print('# Defaulting to `VCF` database.')

	try:
		check_and_select_db(db_connection, args.db)
	except BadDatabase as e:
		print('Bad database:', e)
		exit(1)


	### COMMANDS ###
	if args.command == 'list':
		if not len(args.options) < 2:
			print("The only (optional) argument for this command is a collection name.")
			exit(1)

		## COLLECTIONS LIST ##
		if len(args.options) == 0:
			collection_list = do_list(db_connection)

			print('# Listing all collections:\n')
			print('\n'.join(collection_list))
			print('')
			exit(0)
		#else

		## SINGLE COLLECTION INFO ##
		try:
			metadata = do_list(db_connection, args.options[0])
		except BadCollection as e:
			print('Bad collection:', e)
			exit(1)
		print('# Listing metadata about collection {}:'.format(args.options[0]))
		print('\n### VCF FILES ###')
		for vcf in metadata['vcfs']:
			print(vcf)
		print('\n### SAMPLES ###')
		for sample in metadata['samples']:
			print(sample)
		print('')
		exit(0)


	if args.command == 'check':
		if not len(args.options) == 0:
			print("This command has no parameters.")
			exit(1)

		spurious_meta, spurious_tables, inconsistent_collections = do_check(db_connection)

		print('# Checking consistency state of all collections.\n')
		print('# Spurious collections are missing either metadata')
		print('# or the table containing the actual records.')

		if spurious_meta:
			print('\n### SPURIOUS METADATA ###')
			print('\n'.join(spurious_meta))

		if spurious_tables:
			print('\n### SPURIOUS TABLES ###')
			print('\n'.join(spurious_tables))

		if not spurious_meta and not spurious_tables:
			print('\n### No spurious collections found.')

		if inconsistent_collections:
			print('\n### COLLECTIONS WITH PENDING JOBS ###')
			for collection, state in inconsistent_collections:
				print(collection['id'].ljust(18), '\t',  state )
			print('')
		else:
			print('\n### No collections with pending jobs found.\n')


		print('# Use the fix command without parameters to delete all')
		print('# spurious collections. Use the fix command with a')
		print('# collection name as parameter to undo a pending job over')
		print('# that single collection. For this second case:')
		print('# If the state was `appending`, the tool will remove')
		print('# the data partially imported (basically reverts the')
		print('# failed import without removing the consistent data).')
		print('# If the state was `doing init`, the tool will delete the')
		print('# whole collection, since it would leave it empty in any case.')
		print('')
		print('# Please do make sure that all pending jobs have actually')
		print('# failed and are not still running. It would be rude to ')
		print('# delete a collection currently in use by another process.')
		exit(0)


	if args.command == 'fix':
		if not len(args.options) < 2:
			print("The only (optional) argument for this command is a collection name.")
			exit(1)

		if len(args.options) == 0:
			print('Deleting spurious collections.')
			del_meta, del_tables = do_fix(db_connection)
			if del_meta == del_tables == 0:
				print('All seems fine, nothing to do.')
			else:
				print('Deleted spurious metadata:', del_meta)
				print('Deleted spurious tables:', del_tables)
			exit(0)
		#else

		if not args.f:
			print('WARNING: *THIS OPERATION CANNOT BE UNDONE*')
			print('# Please do make sure that the pending job has actually')
			print('# failed and is not still running. It would be rude to ')
			print('# delete a collection currently in use by another process.\n')
			name = raw_input('To confirm, please type again the name of the collection you want to fix:\n')
			if name != args.options[0]:
				print('Name does not match, aborting.')
				exit(1)

		try:
			result = do_fix(db_connection, args.options[0])
		except BadCollection as e:
			print('Bad collection:', e)
			exit(1)

		if result is None:
			print('This collection is in a consistent state, nothing to do here.')
			exit(0)

		if result == 'doing_init':
			print('This collection had a pending initial import; it has been deleted.')
			exit(0)

		#else
		bad_vcf, bad_samples, deleted_records, reverted_records = result

		print('This collection had a pending append import; it has been reverted by removing the following VCF files:')
		print('\n'.join(bad_vcf))
		print('These are the samples that have been removed:')
		print('\n'.join(bad_samples))

		print('\n\nTotal records: {} deleted, {} reverted.'.format(deleted_records, reverted_records))

		exit(0)


	if args.command == 'rename':
		if not len(args.options) == 2:
			print("Must be called with old and new collection names as parameters (in that order).")
			exit(1)
		print('Sorry, not implemented until RethinkDB offers the functionality directly.')
		print('(https://github.com/rethinkdb/rethinkdb/issues/151)')
		print('In the meantime use copy and delete to do a (yes, *NOT* inplace) rename.')
		exit(0)

	if args.command == 'copy':
		if not len(options) == 2:
			print("Must be called with source and destination collection names as parameters (in that order).")
			exit(1)

		try:
			records = do_copy(db_connection, args.options)
		except BadCollection as e:
			print('Bad collection:', e)
			exit(1)
		print('Total copied records: {}.'.format(records))
		exit(0)


	if args.command == 'delete':
		if not len(options) == 1:
			print("This command requires the name of the collection to be deleted as parameter with an optional `-f` flag.")
			exit(1)
		
		if not args.f:
			print('WARNING: *THIS OPERATION CANNOT BE UNDONE*')
			name = raw_input('To confirm, please type again the name of the collection you want to delete:\n')
			if name != options[0]:
				print('Name does not match, aborting.')
				exit(1)
		try:
			deleted_something = do_delete(db_connection, args.options[0])
		except BadCollection as e:
			print('Bad collection:', e)
			exit(1)

		if not deleted_something:
			print('Collection does not exist, nothing to do here.')
		else:
			print('Collection {} deleted.'.format(args.options[0]))
		exit(0)



def do_list(db, collection=None):
	if collection is None:
		return list(t for t in r.table_list().run(db) if not t.startswith('__'))
	#else
	check_collection_name(collection)

	metadata = r.table('__METADATA__').get(collection).run(db)
	if metadata is None:
		raise BadCollection('collection {} has no metadata.'. format(collection))
	return metadata



def do_check(db):
	metadata = list(r.table('__METADATA__').run(db))
	bad_meta, bad_tables = find_spurious_meta_and_tables(metadata, r.table_list().run(db))

	inconsistent_collections = []
	for m in metadata:
		if str(m['id']) not in bad_meta:
			if m.get('doing_init'):
				inconsistent_collections.append((m, 'doing init'))
			elif m.get('appending_filenames'):
				inconsistent_collections.append((m, 'appending [{}]'.format(', '.join(m['appending_filenames']))))
	
	return bad_meta, bad_tables, inconsistent_collections



def do_fix(db, collection=None):

	if collection is None:
		bad_meta, bad_tables = find_spurious_meta_and_tables(r.table('__METADATA__').run(db), r.table_list().run(db))
		
		if len(bad_meta) == 0 and len(bad_tables) == 0:
			return 0, 0

		r.table('__METADATA__').get_all(*bad_meta).delete().run(db)

		for table in bad_tables:
			r.table_drop(table).run(db)

		return len(bad_meta), len(bad_tables)

	#else
	check_collection_name(collection)

	meta = r.table('__METADATA__').get(collection).run(db)

	if meta is None:
		raise BadCollection('collection {} does not exist.'.format(collection))

	doing_init = meta.get('doing_init')
	appending_filenames = meta.get('appending_filenames')
	


	if not collection in r.table_list().run(db):
		raise BadCollection("this is a spurious collection.")

	if doing_init:
		do_delete(db, collection)
		return 'doing_init'

	if appending_filenames:
		bad_samples = [k for k in meta['samples'] if meta['samples'][k] in appending_filenames]
		result = r.table(collection) \
					.filter(r.row['IDs'].keys().set_intersection(appending_filenames) != [])\
					.replace(lambda x: r.branch(x['IDs'].keys().set_difference(appending_filenames) == [],
						None, # delete record
						x.merge({
							'IDs': r.literal(x['IDs'].without(appending_filenames)),
							'QUALs': r.literal(x['QUALs'].without(appending_filenames)),
							'FILTERs': r.literal(x['FILTERs'].without(appending_filenames)),
							'INFOs': r.literal(x['INFOs'].without(appending_filenames)),
							'samples': r.literal(x['samples'].without(bad_samples)),
							}))).run(db)
		
		r.table('__METADATA__').get(collection)\
			.replace(lambda x: x.merge({
				'vcfs': r.literal(x['vcfs'].without(appending_filenames)),
				'samples': r.literal(x['samples'].without(bad_samples))
				}).without('appending_filenames')).run(db)

		return appending_filenames, bad_samples, result['deleted'], result['replaced']

	return None



def do_copy(db, source, dest):
	check_collection_name(source)
	check_collection_name(dest)

	table_list = r.table_list().run(db)
	source_meta = r.table('__METADATA__').get(source).run(db)
	if not (source in table_list and source_meta is not None):
		raise BadCollection("source collection does not exist.")

	if not (dest not in table_list and r.table('__METADATA__').get(dest).run(db) is None):
		raise BadCollection("destination collection already exists.")

	r.table_create(dest).run(db)
	result = r.table(dest).insert(r.table(source)).run(db)
	source_meta['id'] = dest
	r.table('__METADATA__').insert(source_meta).run(db)

	return result['inserted']



def do_delete(db, collection):
	check_collection_name(collection)

	if not collection in r.table_list().run(db):
		return None

	r.table_drop(collection).run(db)
	r.table('__METADATA__').get(collection).delete().run(db)
	return True




def check_and_select_db(connection, db_name):
	if not re.match(r'^[a-zA-Z0-9_]+$', db_name):
		raise BadDatabase('you can only use alphanumeric characters and underscores for the database name')

	# Database exists?
	if db_name not in r.db_list().run(connection):
		raise BadDatabase('database `{}` does not exist.'.format(db_name))	
	connection.use(db_name)

	# Does this database belong to this application?
	try:
		metadata = r.table('__METADATA__').get('__METADATA__').run(connection)
		assert metadata is not None and metadata.get('application') == 'vcfthink'
	except:
		raise BadDatabase('database `{}` does not belong to this application.'.format(db_name))




def check_collection_name(collection):
	if not re.match(r'^[a-zA-Z0-9_]+$', collection):
		raise BadCollection('you can only use alphanumeric characters and underscores for the database name.')
	if collection.startswith('__'):
		raise BadCollection('collection names starting with double underscores are reserved for internal use.')

def find_spurious_meta_and_tables(metadata, table_list):
	metadata_set = set([x['id'] for x in metadata if type(x['id']) is not list])
	tables = set(table_list)

	bad_meta =  metadata_set - tables
	bad_tables = tables - metadata_set
	return bad_meta, bad_tables


if __name__ == '__main__':
	main()