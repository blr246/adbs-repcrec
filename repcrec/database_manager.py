'''
Database manager.
'''
import copy
import os

class DatabaseManager(object):
	''' The database persistence layer. '''

	class MultiversionClone(object):
		''' A multiversion read consistency clone. '''

		def __init__(self, site, data):
			''' Initialize from a site. '''
			self._site = site
			self._data = copy.deepcopy(data)

		def get_value(self, variable):
			''' Read a variable from the site. '''

			if variable not in self._data:
				raise ValueError(
						'Variable {} is not managed by this site'.format(variable))

			return self._data[variable]

		def has_variable(self, variable):
			''' Check that the site manages a given variable. '''
			return variable in self._data


	def __init__(self,
			variables, data_path, data_file_prefix):
		'''
		Initialize the database.

		Parameters
		----------
		variables : dict
			Dict of variables replicated at this site and their default values.
		data_path : string
			Path where database persistent storage resides.
		data_file_prefix : string
			Prefix for data file.
		'''

		self._data_path = os.path.abspath(data_path)
		if not os.path.isdir(self._data_path):
			raise ValueError(
					'Site data path {} does not exist'.format(self._data_path))
		self._data_file_prefix = data_file_prefix

		self._cache = dict(variables)
		self._write_counter = 0

		# Open site file.
		try:
			self.recover()
		except IOError:
			pass

	def __repr__(self):
		return self._cache.__repr__()

	@property
	def data_path(self):
		''' The site data path. '''
		return self._data_path

	@property
	def data_file_path(self):
		''' Path to site data file. '''
		return os.path.join(
				self._data_path, '{}.dat'.format(self._data_file_prefix))

	@property
	def variables(self):
		''' Get database variables. '''
		return self._cache.keys()

	@property
	def _data_file_tmp_path(self):
		''' Path to site data file. '''
		return os.path.join(
				self._data_path, '{}.tmp'.format(self._data_file_prefix))

	def has_variable(self, variable):
		''' Check that the site manages a given variable. '''
		return variable in self._cache

	def read(self, variable):
		''' Get the value of a variable. '''

		if not self.has_variable(variable):
			raise ValueError(
					'Variable {} is not managed by this site'.format(variable))

		return self._cache[variable]

	def batch_write(self, values):
		''' Write tuples of the form (variable, value). '''

		# Check that all variables are managed by this site.
		for variable, _ in values:
			if not self.has_variable(variable):
				raise ValueError('Variable {} is not managed by this site'
						.format(variable))

		# Update all values in the cache.
		for variable, value in values:
			self._cache[variable] = value

		# Flush always because we don't have a proper log manager here and the
		# database is too small to warrant one.
		self._flush()

	def write(self, variable, value):
		''' Set the value of a variable. '''

		if not self.has_variable(variable):
			raise ValueError(
					'Variable {} is not managed by this site'.format(variable))

		self._cache[variable] = value

		# Flush always because we don't have a proper log manager here and the
		# database is too small to warrant one.
		self._flush()

	def _flush(self):
		''' Flush cached values to site data file. '''

		# First link to a temporary file.
		os.rename(self.data_file_path, self._data_file_tmp_path)
		# Dump to site file.
		self._dump(open(self.data_file_path, 'w'))
		# Remove temporary file.
		os.remove(self._data_file_tmp_path)

	def _dump(self, data_file):
		''' Dump site data to file. '''
		data_file.write(str(self._cache))

	def _read(self, data_file):
		''' Read site data from file. '''

		# This is hilariously unsafe.
		data = eval(data_file.read())
		for variable, value in data.iteritems():
			if not self.has_variable(variable):
				raise ValueError('Variable '
						'{} is not managed by this site'.format(variable))
			self._cache[variable] = value

	def recover(self):
		''' Recover site from disk. '''

		# Recover from standard file.
		try:
			with open(self.data_file_path, 'r') as data_file:
				self._read(data_file)
				return

		except IOError:
			if os.path.isfile(self.data_file_path):
				raise IOError('Failed to access site data file {}'
						.format(self.data_file_path))

		# Recover from tmp file.
		try:
			with open(self._data_file_tmp_path, 'r') as site_tmp_data:
				self._read(site_tmp_data)

				# Try to save the standard site file.
				with open(self.data_file_path, 'w') as data_file:
					self._dump(data_file)
				# Unlink the temporary file.
				os.remove(self._data_file_tmp_path)
				return

		except IOError:
			if os.path.isfile(self._data_file_tmp_path):
				raise IOError('Failed to access site temporary data file {}'
						.format(self._data_file_tmp_path))

		# There is no site file. Initialize it.
		try:
			with open(self.data_file_path, 'w') as data_file:
				self._dump(data_file)
		except IOError:
			raise IOError('Failed to initialize site data file {}'
					.format(self.data_file_path))

	def multiversion_clone(self):
		''' Return a multiversion clone of the site with a read-only interface. '''
		return DatabaseManager.MultiversionClone(self, self._cache)

	def dump(self, variable=None):
		''' Dump database values. '''

		if variable is not None:
			return self.read(variable)
		else:
			return copy.deepcopy(self._cache)


