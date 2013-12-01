'''
Tests for DatabaseManager.
'''

from repcrec import DatabaseManager
import unittest
import time
import os
import random

class DatabaseManagerTest(unittest.TestCase):

	def validate_values(self, dbm, values):
		''' Verify that dbm values match values dict. '''
		for variable, value in values.iteritems():
			self.assertEqual(value, dbm.get_value(variable))

	def setUp(self):
		''' Create test directory. '''

		# Make temp dir.
		now = time.time()
		self._test_dir = os.path.join('/tmp', 'testdbm_{}'.format(now))
		os.makedirs(self._test_dir)
		self._prefix = 'test_site'

		self._values = dict((variable, random.randint(1, 100))
				for variable in range(1, 11))

		self._dbm = DatabaseManager(self._values, self._test_dir, self._prefix)

	def tearDown(self):
		''' Cleanup test directory. '''

		for dirpath, dirnames, filenames in os.walk(
				self._test_dir, topdown=False):
			for filename in filenames:
				os.remove(os.path.join(dirpath, filename))
			for dirname in dirnames:
				os.rmdir(os.path.join(dirpath, dirname))
		os.rmdir(self._test_dir)

	def test_get_set_values(self):
		''' Test that getting and setting values succeeds. '''

		dbm, values = self._dbm, self._values

		self.validate_values(dbm, values)

		# Change all of the values.
		for variable in values:
			new_value = random.randint(101, 200)
			values[variable] = new_value
			dbm.set_value(variable, new_value)

		self.validate_values(dbm, values)

	def test_accssors(self):
		''' Test DatabaseManager accesors. '''

		dbm, values = self._dbm, self._values
		self.assertEqual(values.keys(), dbm.variables)
		self.assertEqual(self._test_dir, dbm.data_path)
		self.assertEqual(
				os.path.join(self._test_dir, '{}.dat'.format(self._prefix)),
				dbm.data_file_path)

	def test_dump(self):
		''' Test dump() function. '''

		dbm, values = self._dbm, self._values
		self.assertEqual(values, dbm.dump())
		for variable, value in values.iteritems():
			self.assertEqual(value, dbm.dump(variable))

	def test_recover(self):
		''' Test that getting and setting values succeeds. '''

		dbm, values = self._dbm, self._values

		# Setup DatabaseManager then delete and re-create it.
		del dbm
		dbm = DatabaseManager(values, self._test_dir, 'test_site')

		self.validate_values(dbm, values)

		# Now move the site file to the temp file and check recovery.
		os.rename(dbm.data_file_path, dbm._data_file_tmp_path)
		del dbm
		dbm = DatabaseManager(values, self._test_dir, 'test_site')

		self.validate_values(dbm, values)

	def test_has_variable(self):
		''' Test that has_variable() works as epxected. '''

		dbm, values = self._dbm, self._values
		for variable in values:
			self.assertTrue(dbm.has_variable(variable))

	def test_multiversion_clone(self):
		''' Test DatabaseManager.MultiversionClone. '''

		dbm, values = self._dbm, self._values

		clone = dbm.multiversion_clone()
		self.validate_values(clone, values)
		for variable in values:
			self.assertTrue(clone.has_variable(variable))


if __name__ == '__main__':
	unittest.main()
