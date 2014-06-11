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
		help='The name of the collection where the VCF files should be stored.')

	parser.add_argument('command', choices=['help', 'list', 'create', 'delete',], 
		help='The operation that must be performed.')
	
	parser.add_argument('options', nargs='+',  
		help='The options for the selected command. Use the help command to know more about each command.')
	

	

	args = parser.parse_args()