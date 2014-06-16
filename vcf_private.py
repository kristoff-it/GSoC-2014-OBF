#!/usr/bin/env python

try:
	import rethinkdb as r
except:
	print('Unable to import the RethinkDB python module.')
	print('To install: pip install rethinkdb')
	print('(You might also want to consider installing the native C++ ProtocolBuffers compiler for better performance)')
	print('\n')
	raise ImportError

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
		help='The name of the collection that we want to query or manage.')

	parser.add_argument('command', choices=['list', 'create', 'delete', 'get'], 
		help='The operation that must be performed.')
	
	parser.add_argument('options', nargs='+',  
		help='The options for the selected command. Use the help command to know more about each command.')

	args = parser.parse_args()


	if args.command == 'help':
		print('Command list:')
		print('help                Show this message and exit.')
		print('')
		print('list                List indexes for privates.')
		print('')
		print('create name [-q, -c] Create a new index for privates')
		print('                     optionally specifying quality or')
		print('                     coverage tresholds.')
		print('')
		print('delete name          Delete an index.')
		print('')
		print('get name +<list> -<list> Query the collection for privates of')
		print('                         +<list> against -<list> using the')
		print('                         specified index. Both groups are')
		print('                         comma-separated lists.')
		print ('')

	if command == 'list':

	if command == 'create':

	if command == 'delete':

	if command == 'get':

