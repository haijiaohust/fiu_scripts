# -*- coding:utf-8 *-*
#######
#this script uses to get the deduplication of fiu trace web-vm
#usage: python fiu_dedup_web-vm.py directory_of_web-vm web-vm_num
#
import os
import sys

if len(sys.argv) != 3:
	print "argv number error"
	os.exit()

request_size_all = 0
request_num_read_bf = 0
request_num_write_bf = 0
request_num_bf = 0
request_num_read_af = 0
request_num_write_af = 0
request_num_af = 0
ref_dist = [0, 0, 0, 0, 0, 0]
time_start = 0
time_end = 0
block_all = 0
block_unique = 0
dict = {}

out_file = open("stat_web-vm.txt", "w+")

def trace_file(path):
	file_object = open(path, "r")
	global request_num_af
	global request_num_read_af
	global request_num_write_af
	while True:
		line = file_object.readline()
		if len(line):
			line_split = line.split(' ')
			block_num = int(line_split[4])
			if(block_num % 8):
				continue
			if(line_split[5] == 'W' or line_split[5] == 'w' ):
				trace_digest = line_split[-1]
				for i in xrange(block_num/8):
					digest_str = trace_digest[32*i:32*i+32]
					digest_ref = dict.get(digest_str)
					if(digest_ref[0] == 1):
						ref_dist[0] += 1
					elif(digest_ref[0] == 2):
						ref_dist[1] += 1
					elif(digest_ref[0] == 3):
						ref_dist[2] += 1
					elif(digest_ref[0] == 4):
						ref_dist[3] += 1
					elif(digest_ref[0] == 5):
						ref_dist[4] += 1
					else:
						ref_dist[5] += 1

					if(digest_ref[1] == 0):
						request_num_write_af += 1
						request_num_af += 1
						dict[digest_str][1] += 1
						buf = str(float(line_split[0])/1000000) + ' ' + line_split[7] + ' ' + \
							str(int(line_split[3])+8*i) + ' ' + '1' + ' ' + line_split[5] + ' ' + \
							str(digest_ref[0]) + '\n'
						out_file.write(buf)
			else:
				request_num_read_af += 1
				request_num_af += 1
		else:
			break
	file_object.close()

def get_trace_reference(path):
	file_object = open(path, "r")
	global request_size_all
	global request_num_read_bf
	global request_num_write_bf
	global request_num_bf
	global time_start
	global time_end
	global block_all
	global block_unique

	while True:
		line = file_object.readline()
		if len(line):
			request_num_bf += 1
			line_split = line.split(' ')
			if(request_num_bf == 1):
				time_start = int(line_split[0])
			if(int(line_split[0]) > time_end):
				time_end = int(line_split[0])
			block_num = int(line_split[4])
			request_size_all += block_num/8
			if(block_num % 8):
				continue
			if(line_split[5] == 'W' or line_split[5] == 'w' ):
				request_num_write_bf += 1
				block_all += block_num/8
				trace_digest = line_split[-1]
				for i in xrange(block_num/8):
					digest_str = trace_digest[32*i:32*i+32]
					if(dict.get(digest_str)):
						dict[digest_str][0] += 1
					else:
						dict[digest_str] = [1, 0]
						block_unique += 1
			else:
				request_num_read_bf += 1
		else:
			break
	file_object.close()

def trace_dir(path, file_num):
	if os.path.isdir(path):
		list_dirs = os.walk(path)
		for root, dirs, files in list_dirs:
			print r'Begin to get fiu_trace web-vm hash_key stat'
			if(file_num == 0):
				file_num = len(files)
			for i in xrange(1, (file_num + 1)):
				trace_file_name = 'webmail+online.cs.fiu.edu-110108-113008.' + str(i) + '.blkparse'
				print trace_file_name
				get_trace_reference(os.path.join(root,trace_file_name))
			print r'End fiu_trace web-vm stat'

			print r'Begin to convert fiu_trace web-vm to disksim trace + reference count'
			for i in xrange(1, (file_num + 1)):
				trace_file_name = 'webmail+online.cs.fiu.edu-110108-113008.' + str(i) + '.blkparse'
				print trace_file_name
				trace_file(os.path.join(root,trace_file_name))
			print r'End fiu_trace web-vm reference count'
	else:
		if os.path.exists(path):
			trace_file(path)
		else:
			print "file path doesn't exist"

trace_dir(sys.argv[1], int(sys.argv[2]))
print 'request_size_all = {}, average_req_size = {:.2}KB'.format(request_size_all, \
	float(request_size_all) / (request_num_bf * 2))
print 'request_num_bf = ', request_num_bf
print 'request_num_read_bf = ', request_num_read_bf
print 'request_num_write_bf = ', request_num_write_bf
print 'request_num_af = ', request_num_af
print 'request_num_read_af = ', request_num_read_af
print 'request_num_write_af = ', request_num_write_af
print 'block_all = ', block_all
print 'block_unique = ', block_unique
print 'the length of dict = ', len(dict)
print 'reference count distribute ', ref_dist
print 'time_start = ', time_start/1000000000
print 'time_end = ', time_end/1000000000
print 'elapse time = ', (time_end - time_start) / 1000000000
print '\n\n'
out_file.close()