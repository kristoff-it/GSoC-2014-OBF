from collections import namedtuple

## TODO: use real regexes for cases where we might be dealing with quoted blocks

## DATA STRUCTURES ##
Record = namedtuple("Record", "CHROM POS ID REF ALT QUAL FILTER INFO samples")
RawRecord = namedtuple("RawRecord", "CHROM POS ID REF ALT QUAL FILTER INFO FORMAT samples")
Headers = namedtuple("Headers", "fileformat infos formats filters alts extra")
#####################


## EXCEPTIONS ##
class BadRecord(Exception):
	pass
class BadField(Exception):
	pass
class BadInfoField(Exception):
	pass
################

# If a field not specified in the headers is found, 
# then an automatic type inferece is tried and, in 
# case of success, the new inferred format is recorded:

# For now all undefined formats are interpreted as variable-length ('.')
# lists of strings. This is because this is an "upcast", meaning that, 
# whlile useless sometimes, we never get it wrong. We could try to "downcast"
# the values to Integer for example, but we might later discover that
# the correct yype was Float, or worse, String. Ending up with an
# inconsistent representation of a field would be awful, so, without
# a better understanding of what might happen, no dangerous conversion is
# attempted.

# TODO: check if single string would be better

inferred_info_fields = {}
inferred_genotype_fields = {}


standard_info_fields = {}

standard_format_fields = {}
# standard_format_fields['GT']	=	[]
# standard_format_fields['FT']	=	[]
# standard_format_fields['GL']	=	[]
# standard_format_fields['GLE']	=	[]
# standard_format_fields['PL']	=	[]
# standard_format_fields['GP']	=	[]
# standard_format_fields['GQ']	=	[]
# standard_format_fields['HQ']	=	[]
# standard_format_fields['PS']	=	[]
# standard_format_fields['PQ']	=	[]
# standard_format_fields['EC']	=	[]
# standard_format_fields['MQ']	=	[]



#
# VCF
#

def parse_vcf(filestream, ignore_bad_infos=False, drop_bad_records=False, dont_parse_fields=False):
	"""Immediately parses the headers, the sample names and returns
	them along with a generator to parse the records. You can optionally
	silently drop bad records, only drop bad info fields or skip the 
	checking (and parsing) of custom fields altogether. Usage:

	>>>> headers, samples, records = parse_vcf(open('myvcf.vcf', 'r'))
	>>>> headers
	Headers(fileformat='4.1', infos={...}, formats={...}, filters={...}, alts={...}, extra={...})
	>>>> headers.infos
	{'AA': ['1', 'String', '"Ancestral Allele"'], ...}
	>>>> samples
	['NA00001', 'NA00002', ...]
	>>>> first_record =  next(records)
	>>>> first_record
	Record(CHROM=20, POS=1337, ID='rs2442', REF='G', ALT=['A', 'T'], QUAL=20.0, FILTER='PASS', INFO={...}, samples=[...])
	>>> first_record.samples
	[{'GT': '0|0', 'GQ': 35, 'DP': 4}, ...]"""

	headers, samples = parse_headers(filestream)
	return headers, samples, parse_records(filestream, headers, ignore_bad_infos, drop_bad_records, dont_parse_fields)



#
# HEADERS
#

#TODO: exceptions
def parse_headers(filestream):
	"""Parses the headers and returns them with the sample namelist.
	Consumes the stream up to where the records begin so you can 
	immediately call parse_records() already from the correct position."""

	line = filestream.readline()
	
	#  0         1 
	#  01234567890123456789
	#  ##fileformat=VCFv4.1
	fileformat = line[17:20]
	assert fileformat in ("4.0", "4.1", "4.2"), \
		"Not a VCF file or not a supported version (4.0, 4.1, 4.2)."

	## HEADERS ##
	headers = Headers(fileformat, {}, {}, {}, {}, {})

	while line.startswith("##"):
		try:
			parse_header_line(line, headers=headers)
		except:
			raise Exception('Error while parsing header line: %s' % line)
		line = filestream.readline()

	# TODO: can't have empty lines... should allow?

	## SAMPLE NAMES ##
	assert line.startswith("#CHROM"), \
		"Was expecting the '#CHROM ...' table header, found something else."
	column_names = line.split('\t')
	assert len(column_names) > 8, \
		"Where are the samples?"

	samples = column_names[9:]
	samples[-1] = samples[-1].strip()

	return headers, samples

#TODO: rewrite
def parse_header_line(line, headers):
	if line.startswith("##INFO"):

		# 0              -
		# 0123456789 ... 1
		# ##INFO=<ID ... >
		ID = Number = Type = Description = None

		subline = line.strip()[8:-1]
		kvpairs = subline.split(',', 3) # dirty hack :3
		for kv in kvpairs:
			key, value = kv.split('=', 1)
			key, value = key.strip(), value.strip()
			if key == "ID":
				ID = value
			elif key == "Number":
				Number = value
			elif key == "Type":
				Type = value
			elif key == "Description":
				Description = value	
			else:
				print key
				assert False

		headers.infos[ID] = [Number, Type, Description]
		return

	if line.startswith("##FORMAT"):

		# 0         1      -
		# 012345678901 ... 1
		# ##FORMAT=<ID ... >
		ID = Number = Type = Description = None

		subline = line.strip()[10:-1]
		kvpairs = subline.split(',', 3)
		for kv in kvpairs:
			key, value = kv.split('=', 1)
			key, value = key.strip(), value.strip()
			if key == "ID":
				ID = value
			elif key == "Number":
				Number = value
			elif key == "Type":
				Type = value
			elif key == "Description":
				Description = value	
			else: 
				print key
				assert False

		headers.formats[ID] = [Number, Type, Description]
		return

	if line.startswith("##FILTER"):

		# 0         1      -
		# 012345678901 ... 1
		# ##FILTER=<ID ... >
		ID = Number = Type = Description = None

		subline = line.strip()[10:-1]
		kvpairs = subline.split(',', 1)
		for kv in kvpairs:
			key, value = kv.split('=', 1)
			key, value = key.strip(), value.strip()
			if key == "ID":
				ID = value
			elif key == "Description":
				Description = value	
			else:
				print key
				assert False

		headers.filters[ID] = [Number, Description]
		return

	if line.startswith("##ALT"):

		# 0         1     -
		# 01234567890 ... 1
		# ##ALT=<ID=D ... >
		ID = Number = Type = Description = None

		subline = line.strip()[10:-1]
		kvpairs = subline.split(',', 1)
		for kv in kvpairs:
			key, value = kv.split('=', 1)
			key, value = key.strip(), value.strip()
			if key == "ID":
				ID = value
			elif key == "Description":
				Description = value	
			else:
				print key
				assert False

		headers.alts[ID] = [Number, Description]
		return


	# DEFAULT:


#
# RECORDS
#

def parse_records(filestream, headers, ignore_bad_infos=False, drop_bad_records=False, dont_parse_fields=False):
	for line in filestream:
		try:
			yield parse_record_line(line, headers, ignore_bad_infos, dont_parse_fields)
		except ValueError:
			if drop_bad_records:
				continue
			raise BadRecord(line)



def parse_record_line(line, headers, ignore_bad_infos, dont_parse_fields):
	fields = line.split('\t')

	if dont_parse_fields:
		return RawRecord(  
						CHROM=fields[0], 
						POS=int(fields[1]), 
						ID=fields[2], 
						REF=fields[3], 
						ALT=fields[4].split(','), 
						QUAL=fields[5],
						FILTER=fields[6], 
						INFO=fields[7].split(';'),
						FORMAT=fields[8].split(':'), 
						samples=tuple([field.split(':') for field in fields[9:]])
					)
	return Record(  
					CHROM=fields[0],
					POS=int(fields[1]), 
					ID=fields[2], 
					REF=fields[3], 
					ALT=fields[4].split(','), 
					QUAL=float(fields[5]),
					FILTER=fields[6], 
					INFO=parse_info_field(fields[7], headers.infos, ignore_bad_infos),
					samples=parse_genotype_fields(fields[8], fields[9:], headers.formats)
				)



def parse_info_field(field, header_infos, ignore_bad_infos):
	parsed_fields = {}

	for kv in field.split(';'):

		try:
			key, value = kv.split('=')
		except ValueError:
			# It's a Flag value
			parsed_fields[kv] = True
			continue


		# Is it a standard field?
		field_definition = standard_info_fields.get(key, None)
		if field_definition is not None:
			try:
				parsed_value = parse_defined_field()
				parsed_fields[key] = parsed_value
			except ValueError:
				if ignore_bad_infos:
					continue
				raise BadInfoField(field)

		# Is it defined in the headers?
		field_definition = header_infos.get(key, None)
		if field_definition is not None:
			try:
				parsed_value = parse_defined_field()
				parsed_fields[key] = parsed_value
			except ValueError:
				if ignore_bad_infos:
					continue
				raise BadInfoField(field)

	return parsed_fields

def parse_genotype_fields(format_field, samples, header_formats):
	fieldnames = format_field.split(':')
	num_fields = len(fieldnames)
	if num_fields == 0: 
		return []	

	first_field_is_GT = int(fieldnames[0] == 'GT')

	parsed_samples = []
	for sample in samples:		
		values = sample.split(':')
		#"Trailing fields can be dropped, if present 
		# the first field must be 'GT' and must be present for each sample."
		assert first_field_is_GT <= len(values) <= num_fields
		
		parsed_fields = {}
		for i in range(len(values)):
			key = fieldnames[i]

			# First check if this is a standard field:
			field_definition = standard_format_fields.get(key, None)
			if field_definition is not None: # hit!
				parsed_value = parse_defined_field(values[i], field_definition)
				parsed_fields[key] = parsed_value
				continue

			# This is not a standard field, try headers:
			field_definition = header_formats.get(key, None)
			if field_definition is not None: #hit!
				parsed_value = parse_defined_field(values[i], field_definition)
				parsed_fields[key] = parsed_value
				continue

			if key not in inferred_formats:
				inferred_formats[key] = ['.', 'String', '"### FIELD WAS NOT DEFINED ###"']

			parsed_fields[key] = values[i].split(',')

			# # Field format is unknown, check if we have already seen it:
			# field_definition = inferred_formats.get(key, None)
			# if field_definition is not None: # already inferred
			# 	try:
			# 		parsed_value = parse_defined_field(values[i], field_definition)
			# 		parsed_fields[key] = parsed_value
			# 	except:
			# 		raise NotImplemented
			# 	continue

		parsed_samples.append(parsed_fields)

	return parsed_samples

def parse_defined_field(field, definition):
	"""Checks the field definition and does the actual
	conversion to the proper type. If value and definition
	are incompatible raises ValueError."""

	# Flags are already unpacked while attempting to split 
	# over '=' when parsing INFO fields.
	# if definition[1] == 'Flag':
	# 	return True

	if definition[0] == '1':
		if definition[1] in ('String', 'Character'):
			return field
		if definition[1] == 'Float':
			return float(field)
		return int(field)
	
	if definition[1] in ('String', 'Character'):
		return field.split(',')

	if definition[1] == 'Float':
		return list(float(x) for x in field.split(','))
	
	return list(int(x) for x in field.split(','))





#
# PARSE & WALK TOGETHER
#


def parse_vcf_together(filestreams):
	headers, samples = parse_headers_together(filestreams)
	return headers, samples, parse_records_together(zip(filestreams, headers))

def parse_headers_together(filestreams):
	return zip(*(parse_headers(f) for f in filestreams))
	

def parse_records_together(fs_headers_touple_list):
	parsers = tuple(parse_records(fs, head) for fs, head in fs_headers_touple_list)

	## RECORDS ##
	record_buffer = list(next(p, None) for p in parsers)
	exhausted_parsers = sum((1 if record is None else 0 for record in record_buffer))
	while exhausted_parsers < len(fs_headers_touple_list):
		selected_records = []
		selected_records_ids = []
		lowest_chrom = lowest_pos = 'ZZZZZZZZZ' #float('inf')

		for i, record in enumerate(record_buffer):
			# a better approach is possible ^^^^
			if record is None:
				continue

			if (record.CHROM == lowest_chrom and record.POS < lowest_pos) \
			    or record.CHROM < lowest_chrom:
				selected_records = [record]
				selected_records_ids = [i]
				lowest_chrom = record.CHROM
				lowest_pos = record.POS

			elif record.CHROM == lowest_chrom and record.POS == lowest_pos:
				selected_records.append(record)
				selected_records_ids.append(i)

		for record_id in selected_records_ids:
			new_record = next(parsers[record_id], None)
			if new_record is None:
				exhausted_parsers += 1
			record_buffer[record_id] = new_record

		yield zip(selected_records_ids, selected_records)







if __name__ == '__main__':
	import gzip, time
	vcf = open('/Users/kappa/github/GSoC-2014-OBF/test/minivcf.vcf', 'r')
	# a = parse_together([vcf])
	pb = False
	h, s, a = parse_vcf_together([vcf])
	print 'RAW fields:', pb
	start_time = time.clock()
	niter = 0
	for x in a:
		niter += 1
	stop_time = time.clock()
	print niter, 'records in', stop_time - start_time, 'seconds;', niter/(stop_time-start_time), 'records per second'

