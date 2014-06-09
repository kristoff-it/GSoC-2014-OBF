#!/usr/bin/env python

import rethinkdb as r

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
	
	parser.add_argument('options', nargs=argparse.REMAINDER,  
		help='The options for the selected command. Use the help command to know more about each command.')
	

	

	args = parser.parse_args()

	if args.command == 'help':
		print 'Command list:'
		print 'help                show this message and exit'
		print ''
		print 'list [collection]   list all collections or detailed'
		print '                    info pertaining a single collection'
		print ''
		print 'check               check consistency status of all '
		print '                    collections'
		print ''
		print 'fix [collection]    fix all spurious collections or'
		print '                    a single inconsistent collection'
		print ''
		print 'rename old new      rename a collection'
		print ''
		print 'copy source dest    create a copy of collection'
		print ''
		print 'delete name [-f]    delete a collection, requires'
		print '                    confirmation if no `-f` specified'
		return

	# from this point onward a db connection is required

	### Input sanity ###
	assert re.match(r'^[a-zA-Z0-9_]+$', args.db) is not None, \
		"You can only use alphanumeric characters and underscores for the database name, aborting."
	####################

	# Connect to RethinkDB
	db_connection = r.connect(host=args.host, port=args.port)

	### Db state check ###
	if args.db == 'VCF':
		print('# Defaulting to `VCF` database.')

	# Database exists?
	assert args.db in r.db_list().run(db_connection), \
		"Database does not exist, aborting."

	db_connection.use(args.db)
		
	# Does this database belong to this application?
	assert '__METADATA__' in r.table_list().run(db_connection), \
		"The database named `{}` does not belong to this application.".format(args.db)
	
	metadata = r.table('__METADATA__').get('__METADATA__').run(db_connection)

	assert metadata is not None and metadata.get('application') == 'vcfthink', \
		"The database named `{}` does not belong to this application.".format(args.db)
	######################

	### COMMANDS ###

	if args.command == 'list':
		return do_list(db_connection, args.options)


	if args.command == 'check':
		return do_check(db_connection, args.options)


	if args.command == 'fix':
		return do_fix(db_connection, args.options)

	if args.command == 'rename':
		assert len(args.options) == 2, \
			"Must be called with old and new collection names as parameters (in that order)."
		print 'Sorry, not implemented until RethinkDB offers the functionality directly.'
		print '(https://github.com/rethinkdb/rethinkdb/issues/151)'
		print 'In the meantime use copy and delete to do a (yes, *NOT* inplace) rename.'
		return

	if args.command == 'copy':
		return do_copy(db_connection, args.options)

	if args.command == 'delete':
		return do_delete(db_connection, args.options)



def do_list(db, options):
	assert len(options) < 2, \
		"The only (optional) argument for this command is a collection name."

	if len(options) == 0:
		print '# Listing all collections:'
		for tablename in r.table_list().run(db):
			if tablename != '__METADATA__':
				print tablename
		return

	metadata = r.table('__METADATA__').get(options[0]).run(db)
	if not metadata:
		print 'Failure, no metadata found for collection {}.'.format(options[0])
		exit(1)

	print '# Listing metadata about collection {}:'.format(options[0])
	print '### VCF FILES ###'
	for vcf in metadata['vcfs']:
		print vcf
	print '### SAMPLES ###'
	for sample in metadata['samples']:
		print sample

	# TODO: print more metadata (creation date, update date, ..., indexes)
	return

def find_spurious_meta_and_tables(metadata, table_list):
	metadata_set = set([x['id'] for x in metadata if type(x['id']) is not list])
	tables = set(table_list)

	bad_meta =  metadata_set - tables
	bad_tables = tables - metadata_set
	return bad_meta, bad_tables


def do_check(db, options):
	# assert len(options) < 2, \
	# 	"The only (optional) argument for this command is a collection name."
	# if len(options) == 0:
	print '# Checking consistency state of all collections.'
	print '# Will now check for spurious collections by looking'
	print '# for mismatches between present tables and metadata.'
	metadata = list(r.table('__METADATA__').run(db))

	bad_meta, bad_tables = find_spurious_meta_and_tables(metadata, r.table_list().run(db))
	if bad_meta:
		print '### SPURIOUS METADATA ###'
		for name in bad_meta:
			print name

	if bad_tables:
		print '### SPURIOUS TABLES ###'
		for name in bad_tables:
			print name

	if not bad_meta and not bad_tables:
		print '# No spurious collections found.'

	print '# Will now check if the remaining collections have pending jobs.'
	printed_header = False
	for collection in metadata:
		if collection.get('doing_init'):
			if not printed_header:
				print '### COLLECTIONS WITH PENDING JOBS ###'
				printed_header = True

			print collection['id'].ljust(18), '\t doing_init'
			continue
		filenames = collection.get('appending_filenames')
		if filenames:
			if not printed_header:
				print '### COLLECTIONS WITH PENDING JOBS ###'
				printed_header = True
			print collection['id'].ljust(18), '\t appending_filenames [{}]'.format(', '.join(filenames))
	if not printed_header:
		print '# No collections with pending jobs found.'


	print '# Use the fix command without parameters to delete all'
	print '# spurious collections. Use the fix command with a'
	print '# collection name as parameter to undo a pending job over'
	print '# that single collection. For this second case:'
	print '# If the state was `appending_filenames`, the tool will'
	print '# remove the data partially imported (basically reverts the'
	print '# failed import without removing the consistent data).'
	print '# If the state was `doing_init`, the tool will delete the'
	print '# whole collection, since it would leave it empty in any case.'
	print ''
	print '# Please do be sure that all pending jobs have actually'
	print '# failed and are not still running. It would be rude to '
	print '# delete a collection currently in use by another process.'
	return

def do_fix(db, options):
	assert len(options) < 2, \
		"The only (optional) argument for this command is a collection name."

	if len(options) == 0:
		print 'Deleting spurious collections.'
		bad_meta, bad_tables = find_spurious_meta_and_tables(r.table('__METADATA__').run(db), r.table_list().run(db))
		
		if len(bad_meta) == 0 and len(bad_tables) == 0:
			print 'All seems fine, nothing to do.'
			return

		r.table('__METADATA__').get_all(*bad_meta).delete().run(db)
		print 'Deleted {} spurious metadata entries.'.format(len(bad_meta))

		for table in bad_tables:
			r.table_drop(table).run(db)

		print 'Deleted {} spurious tables.'.format(len(bad_tables))

		print 'Done.'
		return

	if len(options) == 1:
		print 'Fixing collection {}...'.format(options[0])

		meta = r.table('__METADATA__').get(options[0]).run(db)
		doing_init = meta.get('doing_init')
		appending_filenames = meta.get('appending_filenames')
		
		if meta is None:
			print 'Collection does not exist, aborting.'
			return 

		assert options[0] in r.table_list().run(db), \
			"This is a spurious collection, aborting. Use fix without any parameter to delete it."

		if doing_init == None == appending_filenames:
			print 'This collection is in a consistent state, nothing to do here.'
			return

		if doing_init:
			print 'This collection has not completed the initial import job, will now delete it.'
			return do_delete(db, options + ['-f'])

		if appending_filenames:
			print 'This collection has not completed an import operation that can be reverted without needing to delete the entire collection.'
			print 'The following VCF files will be removed:'
			print '\n'.join(appending_filenames)

			bad_samples = [k for k in meta['samples'] if meta['samples'][k] in appending_filenames]
			print 'The following samples will be removed:'
			print '\n'.join(bad_samples)

			result = r.table(options[0]) \
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
			
			print 'Total records: {} deleted, {} reverted.'.format(result['deleted'], result['replaced'])

			r.table('__METADATA__').get(options[0])\
				.replace(lambda x: x.merge({
					'vcfs': r.literal(x['vcfs'].without(appending_filenames)),
					'samples': r.literal(x['samples'].without(bad_samples))
					}).without('appending_filenames')).run(db)

			print 'Done.'






def do_copy(db, options):
	assert len(options) == 2, \
		"Must be called with source and destination collection names as parameters (in that order)."
	source, dest = options
	table_list = r.table_list().run(db)
	source_meta = r.table('__METADATA__').get(source).run(db)
	assert source in table_list and source_meta is not None, \
		"Source collection does not exist."

	assert dest not in table_list and r.table('__METADATA__').get(dest).run(db) is None, \
		'Destination collection already exists.'

	r.table_create(dest).run(db)
	result = r.table(dest).insert(r.table(source)).run(db)
	print 'Total copied records:', result['inserted'],
	source_meta['id'] = dest
	r.table('__METADATA__').insert(source_meta).run(db)

	print ' - done!'


def do_delete(db, options):
	assert len(options) == 1 or (len(options) == 2 and '-f' in options), \
		"This command requires the name of the collection to be deleted as parameter with an optional `-f` flag."

	force = False
	if len(options) == 2:
		force = True
		options.remove('-f')


	if not options[0] in r.table_list().run(db):
		print 'Collection does not exist, nothing to do here.'
		return

	if not force:
		name = raw_input('WARNING: *THIS OPERATION CANNOT BE REVERTED* \nTo confirm, please type again the name of the collection you want to delete:\n')
		if name != options[0]:
			print 'Name does not match, aborting.'
			exit(1)

	r.table_drop(options[0]).run(db)
	r.table('__METADATA__').get(options[0]).delete().run(db)
	print 'Deleted collection `{}`.'.format(options[0])





if __name__ == '__main__':
	main()