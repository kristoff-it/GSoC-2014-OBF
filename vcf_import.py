import gzip
import rethinkdb as r
from vcf_miniparser import parse_together



def load(collection, vcf_filenames):
	db = connect_to_db()
	collection_status = get_collection_status(collection, db=db)
	filestreams = []
	for filename in vcf_filenames:
		if filename.endswith(".gz"):
			filestreams.append(gzip.open(filename, 'r'))
		else:
			filestreams.append(open(filename, 'r'))
	parsers = parse_together(filestreams)

	headers = next(parsers, None)
	samples = next(parsers, None)
	
	flattened_samples = tuple([sample for sublist in samples for sample in sublist])
	assert len(flattened_samples) == len(set(flattened_samples)), \
		"Some sample names are colliding!"

	print "headers", len(headers)
	print "filenames", len(vcf_filenames)
	print "samples", len(samples)
	print samples

	## STORE METADATA ##
	collection_info = {
		'id': collection,
		'vcfs': {vcf_filenames[i] : headers[i]._asdict() for i in range(len(headers))},
		'samples': {sample: vcf_filenames[i] for i in range(len(headers)) for sample in samples[i]}
	}

	print collection_info
	r.table('METADATA').insert(collection_info).run(db)

	## STORE ROWS ##
	r.table_create(collection).run(db)

	for multirecord in parsers:
		r.table(collection).insert(merge_records(multirecord, vcf_filenames, samples)).run(db)
		


def merge_records(lines, vcf_filenames, samples):
	CHROM = lines[0].CHROM
	POS = lines[0].POS
	IDs = {}
	REF = lines[0].REF
	# ALT = []
	QUALs = {}
	FILTERs = {}
	INFOs = {}
	FORMATs = {}
	samples = {}

	for i, record in lines:
		IDs[vcf_filenames[i]] = record.ID
		# merge alts
		QUALs[vcf_filenames[i]] = record.QUAL
		FILTERs[vcf_filenames[i]] = record.FILTER
		INFOs[vcf_filenames[i]] = record.INFO
		FORMATs[vcf_filenames[i]] = record.FORMATs
		samples = {samples[i][k]: match_sample(lines[i], k) for i in range(len(lines)) for k in range(len(lines[i].samples))}

	return {
		'CHROM': CHROM,
		'POS': POS,
		'IDs': IDs,
		'REF': REF,
		'QUALs': QUALs,
		'FILTERs': FILTERs,
		'INFOs': INFOs,
		'FORMATs': FORMATs,
		'samples': samples
	}

def match_sample(record, index):
	











def connect_to_db():
	# should perform some sanity checks
	# and have some other options

	db = r.connect(db='gsoc')

	return db

def get_collection_status(collection, db):
	# should gather info about the collection
	return {}


if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(description='Load some VCF files.')


	parser.add_argument('collection', metavar='collection', type=str, nargs=1, 
		help='the name of the collection where the VCF files should be stored')

	parser.add_argument('vcf_filenames', metavar='file', type=str, nargs='+',
		help='a VCF file')

	args = parser.parse_args()

	load(args.collection, args.vcf_filenames)


