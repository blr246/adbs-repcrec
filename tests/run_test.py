'''
Run test for standard repcrec.
'''
from repcrec import *

import argparse
import os

def cleanup_dir(data_dir):
	''' Cleanup test directory. '''

	for dirpath, dirnames, filenames in os.walk(
			data_dir, topdown=False):
		for filename in filenames:
			os.remove(os.path.join(dirpath, filename))
		for dirname in dirnames:
			os.rmdir(os.path.join(dirpath, dirname))
	os.rmdir(data_dir)


def main():
	''' Main method. '''

	argument_parser = argparse.ArgumentParser()
	argument_parser.add_argument('DATA_DIR',
			help='Path test data.')
	argument_parser.add_argument('TEST_FILE_PATH',
			help='Path to test file.')

	args = argument_parser.parse_args()

	data_dir = os.path.abspath(args.DATA_DIR)
	test_file_path = os.path.abspath(args.TEST_FILE_PATH)

	if os.path.isdir(data_dir):
		raise ValueError('Data dir {} exists'.format(data_dir))

	print 'Running test file {} using data dir {}'.format(
			test_file_path, data_dir)

	print 'Test file contents:'
	with open(test_file_path, 'r') as test_file:
		print test_file.read()

	try:
		os.makedirs(data_dir)
		test_file = TestFile(test_file_path)

		data_file_map = dict((index, dict((variable, 10 * variable)
			for variable in it.ifilter(
				lambda x: (0 == (x & 1)) or (index == (1 + (x / 2))),
				range(1, 21))))
			for index in range(1, 11))

		transaction_manager = TransactionManager(data_file_map, data_dir)

		for commands in test_file:
			transaction_manager.send_commands(commands)

		# Check assertions.
		commit_abort_log = transaction_manager.get_commit_abort_log()
		test_file.assert_debug_commands(commit_abort_log)

	finally:
		cleanup_dir(data_dir)

if __name__ == '__main__':
	main()

