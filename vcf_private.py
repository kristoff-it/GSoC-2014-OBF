#!/usr/bin/env python

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

	if r.protobuf_implementation != 'cpp':
		print('# Info: you might want to install the native C++ ProtocolBuffers compiler for better performance.')
		print('# For more information, see: http://www.rethinkdb.com/docs/driver-performance/')

	parser = argparse.ArgumentParser(description='Manage the VCF database.')

	parser.add_argument('--host', default='localhost', 
		help='Host address where the RethinkDB instance is running. Defaults to `localhost`.')

	parser.add_argument('--port', default=28015, 
		help='Port number where the RethinkDB instance is listening. Defaults to `28015`.')

	parser.add_argument('--db', default='VCF', 
		help='Database name where the VCF data is stored. Defaults to `VCF`.')

	parser.add_argument('collection', 
		help='Name of the collection that you want to query or whose indexes manage.')


	subparsers = parser.add_subparsers(dest='command')

	## LIST ##
	parser_list = subparsers.add_parser('list', 
		help='List all indexes in collection.')

	## CREATE ##
	parser_create = subparsers.add_parser('create', 
		help='Create a new index optionally specifying quality or coverage tresholds.')
	parser_create.add_argument('name',  
		help='Name of the new index.')
	parser_create.add_argument('--min-coverage', type=int,
		help='Calls whose coverage is under this value are ignored.')
	parser_create.add_argument('--min-quality', type=int,
		help='Calls whose quality is under this value are ignored.')
	parser_create.add_argument('--only-SNPs', action='store_true',
		help='Index only variants that are SNPs (both REF and ALT values are single nucleotides).')
	parser_create.add_argument('--apply-filters', action='store_true',
		help='Index only variants that have `PASS` or `.` as FILTER values.')

	## DELETE ##
	parser_delete = subparsers.add_parser('delete', 
		help='Delete an index.')
	parser_delete.add_argument('name',  
		help='Index name.')

	## GET ##
	parser_get = subparsers.add_parser('get', 
		help='Get a VCF containing only private SNPs. Must query a previously built index.')
	parser_get.add_argument('name',  
		help='Index name.')
	parser_get.add_argument('sample', metavar='samples',  nargs='+',
		help='Sample names that form the group whose privates must be returned.')
	parser_get.add_argument('--ignore',  nargs='+',
		help='Privates of `plus_group` are evaluated against all remaining samples in the collection. To exclude some samples from this second group list them here.')
	parser_get.add_argument('--merge',  
		help='nome indice')

	args = parser.parse_args()


	print args


	# Connect to RethinkDB
	db_connection = r.connect(host=args.host, port=args.port)

	if args.db == 'VCF':
		print('# Defaulting to `VCF` database.')

	try:
		check_and_select_db(db_connection, args.db)
	except BadDatabase as e:
		print('Bad database:', e)
		exit(1)



	if args.command == 'list':
		try:
			print r.table(args.collection).index_list().run(db_connection)
		except:
			raise BadCollection('collection {} does not exist.'.format(args.collection))
		exit(1)

	if args.command == 'create':


		for x in list(r.table(args.collection).map( 
			lambda record: record['samples'].keys().group(
				lambda sample: record['samples'].get_field(sample)['GT']).ungroup()['reduction']
			).run(db_connection)):
			print ''
			for y in x:
				print ':::', y
			print ''

		print r.table(args.collection).index_create(args.name, 
			lambda record: record['samples'].keys().group(
				lambda sample: record['samples'].get_field(sample)['GT']).ungroup()['reduction']
			, multi=True).run(db_connection)

		exit(0)





	# if command == 'delete':

	if args.command == 'get':
		import json

		for x in r.table(args.collection).get_all(args.sample, index=args.name).run(db_connection):
			print ''
			print x['id'], ':'
			print json.dumps(x['samples'], sort_keys=True, indent=2)
			print ''


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


if __name__ == '__main__':
	main()

