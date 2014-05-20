from collections import namedtuple


def parse(filestream):
	line = filestream.readline()
	
	#  0         1 
	#  01234567890123456789
	#  ##fileformat=VCFv4.1
	fileformat = line[17:20]

	if fileformat in ("4.0", "4.1", "4.2"):

		## HEADERS ##
		Headers = namedtuple("Headers", "fileformat infos formats filters alts extra")
		headers = Headers(fileformat, {}, {}, {}, {}, {})

		while line.startswith("##"):
			parse_header(line, headers=headers)
			line = filestream.readline()
		yield headers

		

		## SAMPLE NAMES ##
		assert line.startswith("#CHROM"), \
			"Was expecting the '#CHROM ...' table header, found something else."
		column_names = line.split('\t')
		assert len(column_names) > 8, \
			"This VCF file does not contain any genotype data!"

		samples = column_names[9:]
		samples[-1] = samples[-1].strip()

		yield samples

		

		## RECORDS ##
		Record = namedtuple("Record", "CHROM POS ID REF ALT QUAL FILTER INFO FORMAT samples") 

		for line in filestream:
			try:
				yield parse_line(line, Record=Record)
			except:
				print "ZOMG"
				break


def parse_header(line, headers):
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





def parse_line(line, Record):
	fields = line.split('\t')

	return Record(  
					CHROM=fields[0], 
					POS=fields[1], 
					ID=fields[2], 
					REF=fields[3], 
					ALT=fields[4].split(','), 
					QUAL=fields[5],
					FILTER=fields[6], 
					INFO=fields[7].split(';'), 
					FORMAT=fields[8].split(':'), 
					samples=tuple([field.split(':') for field in fields[9:]])
				)


### PARSE & WALK TOGETHER ###

def parse_together(filestreams):
	parsers = tuple(parse(f) for f in filestreams)

	## HEADERS ##
	yield list(next(p, None) for p in parsers)

	## SAMPLE NAMES ##
	yield list(next(p, None) for p in parsers)


	## RECORDS
	line_buffer = list(next(p, None) for p in parsers)
	exhausted_parsers = sum((1 if line is None else 0 for line in line_buffer))
	while exhausted_parsers < len(filestreams):
		selected_lines = []
		selected_lines_ids = []
		lowest_chrom = lowest_pos = 'ZZZZZZZZZ' #float('inf')

		for i, line in enumerate(line_buffer):
			# a better approach is possible ^^^^
			if line is None:
				continue

			if (line.CHROM == lowest_chrom and line.POS < lowest_pos) \
			    or line.CHROM < lowest_chrom:
				selected_lines = [line]
				selected_lines_ids = [i]
				lowest_chrom = line.CHROM
				lowest_pos = line.POS

			elif line.CHROM == lowest_chrom and line.POS == lowest_pos:
				selected_lines.append(line)
				selected_lines_ids.append(i)

		for line_id in selected_lines_ids:
			new_line = next(parsers[line_id], None)
			if new_line is None:
				exhausted_parsers += 1
			line_buffer[line_id] = new_line

		yield zip(selected_lines_ids, selected_lines)







if __name__ == '__main__':
	import gzip, time
	vcf = open('/Users/kappa/github/GSoC-2014-OBF/test/minivcf.vcf', 'r')
	a = parse_together([vcf])
	start_time = time.clock()
	niter = 0
	for x in a:
		niter += 1
	stop_time = time.clock()
	print niter, 'records in', stop_time - start_time, 'seconds;', niter/(stop_time-start_time), 'records per second'

