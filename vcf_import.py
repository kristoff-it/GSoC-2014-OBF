import gzip
import rethinkdb as r
from vcf_miniparser import parse_vcf_together
import itertools


# TODOS
# check db status
# add other switches
#


def load(collection, vcf_filenames):
	db = connect_to_db()
	collection_status = get_collection_status(collection, db=db)
	filestreams = []
	for filename in vcf_filenames:
		if filename.endswith(".gz"):
			filestreams.append(gzip.open(filename, 'r'))
		else:
			filestreams.append(open(filename, 'r'))
	headers, samples, parsers = parse_vcf_together(filestreams)
	
	flattened_samples = tuple([sample for sublist in samples for sample in sublist])
	assert len(flattened_samples) == len(set(flattened_samples)), \
		"Some sample names are colliding!"


	## STORE METADATA ##
	collection_info = {
		'id': collection,
		'vcfs': {vcf_filenames[i] : headers[i]._asdict() for i in range(len(headers))},
		'samples': {sample: vcf_filenames[i] for i in range(len(headers)) for sample in samples[i]}
	}

	r.table('METADATA').insert(collection_info).run(db)

	## STORE ROWS ##
	r.table_create(collection[0]).run(db)

	for multirecord in parsers:
		# pass
		r.table(collection[0]).insert(merge_records(multirecord, vcf_filenames, samples), durability='soft').run(db)
		


def merge_records(multirecord, vcf_filenames, sample_names):
	CHROM = multirecord[0][1].CHROM
	POS = multirecord[0][1].POS
	IDs = {}
	REF = multirecord[0][1].REF
	# ALT = []
	QUALs = {}
	FILTERs = {}
	INFOs = {}
	samples = {}

	for i, record in multirecord:
		IDs[vcf_filenames[i]] = record.ID
		# merge alts
		QUALs[vcf_filenames[i]] = record.QUAL
		FILTERs[vcf_filenames[i]] = record.FILTER
		INFOs[vcf_filenames[i]] = record.INFO
		samples.update([(sample_names[i][k], sample_data) for (k, sample_data) in enumerate(record.samples)])

	return {
		'CHROM': CHROM,
		'POS': POS,
		'IDs': IDs,
		'REF': REF,
		'QUALs': QUALs,
		'FILTERs': FILTERs,
		'INFOs': INFOs,
		# 'FORMATs': FORMATs,
		'samples': samples
	}





def connect_to_db():
	# should perform some sanity checks
	# and have some other options

	db = r.connect(db='gsoc')

	return db

def get_collection_status(collection, db):
	# should gather info about the collection
	return {}


if __name__ == '__main__':
	import argparse, time

	parser = argparse.ArgumentParser(description='Load some VCF files.')


	parser.add_argument('collection', metavar='collection', type=str, nargs=1, 
		help='the name of the collection where the VCF files should be stored')

	parser.add_argument('vcf_filenames', metavar='file', type=str, nargs='+',
		help='a VCF file')

	args = parser.parse_args()

	start_time = time.clock()
	load(args.collection, args.vcf_filenames)
	stop_time = time.clock()

	print 'Loaded all records in', stop_time - start_time, 'seconds.'


