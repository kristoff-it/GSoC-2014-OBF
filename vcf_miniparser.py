from __future__ import print_function
from collections import namedtuple


# TODO: remove state from module
# TODO: better error control


## DATA STRUCTURES ##
Record = namedtuple("Record", "CHROM POS ID REF ALT QUAL FILTER INFO samples")
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

# Unsafe typecasting might lead to unwanted behaviours that could 
# be difficult to debug for the common user, for this reason all 
# undefined formats are interpreted as variable-length ('.') lists 
# of strings. 

inferred_infos = {}
inferred_formats = {}


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

def parse_vcf(filestream, ignore_bad_info=False, drop_bad_records=False):
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
	return headers, samples, parse_records(filestream, headers, ignore_bad_info, drop_bad_records)



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
	fileformat = line[13:20]
	assert fileformat in ("VCFv4.0", "VCFv4.1", "VCFv4.2"), \
		"Not a VCF file or not a supported version (4.0, 4.1, 4.2)."

	## HEADERS ##
	headers = Headers(fileformat, {}, {}, {}, {}, {})

	while line.startswith("##"):
		try:
			parse_header_line(line, headers=headers)
		except:
			raise Exception('Error while parsing header line: {}'.format(line))
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



def parse_header_line(line, headers):
	if line.startswith("##INFO"):

		# 0              -
		# 0123456789 ... 1
		# ##INFO=<ID ... >
		ID = Number = Type = Description = None

		subline = line.strip()[8:-1]
		idkv, numkv, typekv, desckv = subline.split(',', 3) # dirty hack :3
		# ^ all fields must be present, its fine to fail if there is a mismatch

		# ID
		key, value = idkv.split('=')
		assert key == 'ID'
		ID = value

		# Number
		key, value = numkv.split('=')
		assert key == 'Number'
		Number = value

		# Type
		key, value = typekv.split('=')
		assert key == 'Type'
		Type = value

		# Description
		key, value = desckv.split('=', 1)
		assert key == 'Description'
		Description = value

		headers.infos[ID] = [Number, Type, Description]
		return


	if line.startswith("##FORMAT"):

		# 0         1      -
		# 012345678901 ... 1
		# ##FORMAT=<ID ... >
		ID = Number = Type = Description = None

		subline = line.strip()[10:-1]
		idkv, numkv, typekv, desckv = subline.split(',', 3)
		# ^ all fields must be present, its fine to fail if there is a mismatch
		
		# ID
		key, value = idkv.split('=')
		assert key == 'ID'
		ID = value

		# Number
		key, value = numkv.split('=')
		assert key == 'Number'
		Number = value

		# Type
		key, value = typekv.split('=')
		assert key == 'Type'
		Type = value

		# Description
		key, value = desckv.split('=', 1)
		assert key == 'Description'
		Description = value

		headers.formats[ID] = [Number, Type, Description]
		return


	if line.startswith("##FILTER"):

		# 0         1      -
		# 012345678901 ... 1
		# ##FILTER=<ID ... >
		ID = Number = Description = None

		subline = line.strip()[10:-1]
		idkv, numkv, desckv = subline.split(',', 1)
		# ^ all fields must be present, its fine to fail if there is a mismatch

		# ID
		key, value = idkv.split('=')
		assert key == 'ID'
		ID = value

		# Number
		key, value = numkv.split('=')
		assert key == 'Number'
		Number = value

		# Description
		key, value = desckv.split('=', 1)
		assert key == 'Description'
		Description = value

		headers.filters[ID] = [Number, Description]
		return


	if line.startswith("##ALT"):

		# 0         1     -
		# 01234567890 ... 1
		# ##ALT=<ID=D ... >
		ID = Number = Description = None

		subline = line.strip()[10:-1]
		idkv, numkv, desckv = subline.split(',', 1)
		# ^ all fields must be present, its fine to fail if there is a mismatch

		# ID
		key, value = idkv.split('=')
		assert key == 'ID'
		ID = value

		# Number
		key, value = numkv.split('=')
		assert key == 'Number'
		Number = value

		# Description
		key, value = desckv.split('=', 1)
		assert key == 'Description'
		Description = value

		headers.alts[ID] = [Number, Description]
		return


#
# RECORDS
#

def parse_records(filestream, headers, ignore_bad_info=False, drop_bad_records=False):
	for line in filestream:
		try:
			yield parse_record_line(line, headers, ignore_bad_info)
		except ValueError:
			if drop_bad_records:
				continue
			#else
			raise BadRecord(line)



def parse_record_line(line, headers, ignore_bad_info):
	fields = line.split('\t')

	return Record(  
					CHROM=fields[0],
					POS=int(fields[1]), 
					ID=fields[2], 
					REF=fields[3], 
					ALT=fields[4].split(','), 
					QUAL=float(fields[5]),
					FILTER=fields[6], 
					INFO=parse_info_field(fields[7], headers.infos, ignore_bad_info),
					samples=parse_genotype_fields(fields[8], fields[9:], headers.formats)
				)


bad_info_fields = {}
def parse_info_field(field, header_infos, ignore_bad_info):
	parsed_fields = {}

	if field == '.':
		return {}

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
				parsed_value = parse_defined_field(value, field_definition)
				parsed_fields[key] = parsed_value
			except ValueError:
				if ignore_bad_info:
					if key not in bad_info_fields:
						bad_info_fields[key] = True
						print('Warning: field `{}` does not respect its type, dropping it. This is to prevent inconsistent results from queries. Will not notify ulterior errors with the same field ID.'.format(kv))
					continue
				raise BadInfoField(kv)

		# Is it defined in the headers?
		field_definition = header_infos.get(key, None)
		if field_definition is not None:
			try:
				parsed_value = parse_defined_field(value, field_definition)
				parsed_fields[key] = parsed_value
			except ValueError:
				if ignore_bad_info:
					if key not in bad_info_fields:
						bad_info_fields[key] = True
						print('Warning: field `{}` does not respect its type, dropping it. This is to prevent inconsistent results from queries. Will not notify ulterior errors with the same field ID.'.format(kv))
					continue
				raise BadInfoField(kv)

		# Undefined field
		if key not in inferred_infos:
			inferred_infos[key] = ['.', 'String', '"### FIELD WAS NOT DEFINED ###"']

		parsed_fields[key] = value.split(',')


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
		#"Trailing fields can be dropped, if present, the first 
		# field must be 'GT' and must be present for each sample."
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

	if field == '.':
		return ['.']

	if definition[0] == '1':

		if definition[1] in ('String', 'Character'):
			return field

		if field == '.':
			return None

		if definition[1] == 'Float':
			return float(field)
		return int(field)
	
	if definition[1] in ('String', 'Character'):
		return field.split(',')

	if definition[1] == 'Float':
		return list(None if x == '.' else float(x) for x in field.split(','))
	
	return list(None if x == '.' else int(x) for x in field.split(','))


#
# PARSE & WALK TOGETHER
#


def parse_vcf_together(filestreams, ignore_bad_info=False):
	headers, samples = parse_headers_together(filestreams)
	return headers, samples, parse_records_together(zip(filestreams, headers), ignore_bad_info=ignore_bad_info)

def parse_headers_together(filestreams):
	return zip(*(parse_headers(f) for f in filestreams))
	

def parse_records_together(fs_headers_touple_list, ignore_bad_info=False):
	parsers = tuple(parse_records(fs, head, ignore_bad_info=ignore_bad_info) for fs, head in fs_headers_touple_list)

	## RECORDS ##
	record_buffer = list(next(p, None) for p in parsers)
	exhausted_parsers = sum((1 if record is None else 0 for record in record_buffer))
	while exhausted_parsers < len(fs_headers_touple_list):
		selected_records = []
		selected_records_ids = []
		lowest_chrom = '~~~~~~~~~~~~~~~~~~'
		# ~ is the highest ascii valued printable character, lexicographical max_value somehow
		lowest_pos = float('inf')

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
	pass
	# TODO: tests

