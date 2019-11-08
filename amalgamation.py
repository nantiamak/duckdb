# this script creates a single header + source file combination out of the DuckDB sources
header_file = "duckdb.hpp"
source_file = "duckdb.cpp"
cache_file = 'amalgamation.cache'
import os, re, sys, pickle

compile = False
resume = False

for arg in sys.argv:
	if arg == '--compile':
		compile = True
	elif arg == '--resume':
		resume = True

if not resume:
	try:
		os.remove(cache_file)
	except:
		pass

def get_includes(fpath):
	with open(fpath, 'r') as f:
		text = f.read()
	return ["src/include/" + x for x in re.findall("[#]include [\"](duckdb/[^\"]+)", text)]

def cleanup_file(text):
	# remove all includes of duckdb headers
	text = re.sub("[#]include [\"]duckdb[^\n]+", "", text)
	# remove all "#pragma once" notifications
	text = re.sub('#pragma once', '', text)
	text = re.sub('\n+', '\n', text)
	return text

# recursively get all includes and write them
written_headers = {}

excluded_files = ["duckdb-c.cpp"]

def write_file(current_file, hfile):
	if current_file.split('/')[-1] in excluded_files:
		print(current_file)
		return
	if current_file in written_headers:
		# header is already written
		return
	written_headers[current_file] = True

	print(current_file)

	# find includes of this header
	includes = get_includes(current_file)
	# now write all the dependencies of this header first
	for include in includes:
		write_file(include, hfile)
	# now read the header and write it
	with open(current_file, 'r') as f:
		hfile.write(cleanup_file(f.read()))

def try_compilation(fpath, cache):
	if fpath in cache:
		return
	print(fpath)
	cmd = 'clang++ -std=c++11 -S -MMD -MF dependencies.d -o deps.s ' + fpath + ' -Isrc/include -Ithird_party/hyperloglog -Ithird_party/re2 -Ithird_party/miniz -Ithird_party/libpg_query/include -Ithird_party/libpg_query'
	ret = os.system(cmd)
	if ret != 0:
		raise Exception('Failed compilation of file "' + fpath + '"!')
	cache[fpath] = True
	with open(cache_file, 'wb') as cf:
		pickle.dump(cache, cf)

def compile_dir(dir, cache):
	files = os.listdir(dir)
	for fname in files:
		fpath = os.path.join(dir, fname)
		if os.path.isdir(fpath):
			compile_dir(fpath, cache)
		elif fname.endswith('.cpp') or fname.endswith('.hpp'):
			try_compilation(fpath, cache)

if compile:
	# compilation pass only
	# compile all files in the src directory (including headers!) individually
	try:
		with open(cache_file, 'rb') as cf:
			cache = pickle.load(cf)
	except:
		cache = {}
	compile_dir('src', cache)
	exit(0)

# now construct duckdb.hpp from these headers
print("-----------------------")
print("-- Writing duckdb.hpp --")
print("-----------------------")
with open(header_file, 'w+') as hfile:
	hfile.write("#pragma once")
	write_file('src/include/duckdb.hpp', hfile)

def write_dir(dir, sfile):
	files = os.listdir(dir)
	for fname in files:
		fpath = os.path.join(dir, fname)
		if os.path.isdir(fpath):
			write_dir(fpath, sfile)
		elif fname.endswith('.cpp'):
			write_file(fpath, sfile)

# now construct duckdb.cpp
print("------------------------")
print("-- Writing duckdb.cpp --")
print("------------------------")

# scan all the .cpp files
with open(source_file, 'w+') as sfile:
	sfile.write('#include "duckdb.hpp"\n\n')
	write_dir('src', sfile)
