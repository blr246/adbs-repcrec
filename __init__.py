'''
Modules supporting Replicated Concurrency Control and Recovery (RepCRec)
database project.

Description
-----------
The data consists of 20 distinct variables x1, ..., x20. There are 10 sites
numbered 1 to 10. A copy is indicated by a dot. Thus, x6.2 is the copy of
variables x6 at site 2. The odd indexed variables are at one site each (i.e. 1
+ index number mod 10). Even indexed variables are at all sites. Each variable
is initialized to the value 10i. Each site has an independent lock table. If
that site fails, the lock table is erased.
'''
import os
import sys
import copy
import itertools as it
import StringIO

class Site(object):
	''' Represents a database site. '''

	def __init__(self, index, variables, data_path, flush_after_writes=10):
		'''
		Initialize the site.

		Parameters
		----------
		index : integer
			The integer index of the site.
		variables : dict
			Dict of variables replicated at this site and their default values.
		data_path : string
			Path where site data file resides.
		flush_after_writes : integer
			Number of writes to wait before flushing to disk.
		'''

		self._database_manager = DatabaseManager(
				variables, data_path, 'site_{}'.format(index),
				flush_after_writes)
		self._lock_manager = LockManager(self._database_manager)
		self._index = index

	def __repr__(self):
		return '{{ \'index\': {}, \'data\': {}, \'locks\': {} }}'.format(
				self._index, self._database_manager, self._lock_manager)

	def dump(self, variable=None):
		''' Dump site data. '''
		return self._database_manager.dump(variable)

	@property
	def index(self):
		''' The site index. '''
		return self._index


class LockManager(object):
	''' Database lock manager. '''

	_UNLOCKED, _LOCKED = 0, 1

	_LOCK_TABLE_STATES = {
			_UNLOCKED: 'UNLOCKED',
			_LOCKED: 'LOCKED',
			}

	def __init__(self, database_manager):
		'''
		Initialize the lock manager.

		Parameters
		----------
		database_manager : DatabaseManager
			The database manager whose locks we manage.
		'''
		self._lock_table = dict((variable, self._UNLOCKED)
				for variable in database_manager.variables)

	def __repr__(self):
		return str(dict((variable, self._LOCK_TABLE_STATES[state])
			for variable, state in self._lock_table.iteritems()))

class DatabaseManager(object):
	''' The database. '''

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
			variables, data_path, data_file_prefix, flush_after_writes=10):
		'''
		Initialize the database.

		Parameters
		----------
		variables : dict
			Dict of variables replicated at this site and their default values.
		data_path : string
			Path where site data file resides.
		data_file_prefix : string
			Prefix for data file.
		flush_after_writes : integer
			Number of writes to wait before flushing to disk.
		'''

		self._data_path = os.path.abspath(data_path)
		if not os.path.isdir(self._data_path):
			raise ValueError(
					'Site data path {} does not exist'.format(self._data_path))
		self._data_file_prefix = data_file_prefix

		self._cache = dict(variables)
		self._flush_after_writes = flush_after_writes
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
	def site_file_path(self):
		''' Path to site data file. '''
		return os.path.join(
				self._data_path, '{}.dat'.format(self._data_file_prefix))

	@property
	def variables(self):
		''' Get database variables. '''
		return self._cache.keys()

	@property
	def _site_tmp_file_path(self):
		''' Path to site data file. '''
		return os.path.join(
				self._data_path, '{}.tmp'.format(self._data_file_prefix))

	def has_variable(self, variable):
		''' Check that the site manages a given variable. '''
		return variable in self._cache

	def get_value(self, variable):
		''' Get the value of a variable. '''

		if not self.has_variable(variable):
			raise ValueError(
					'Variable {} is not managed by this site'.format(variable))

		return self._cache[variable]

	def set_value(self, variable, value):
		''' Set the value of a variable. '''

		if not self.has_variable(variable):
			raise ValueError(
					'Variable {} is not managed by this site'.format(variable))

		self._cache[variable] = value

		# Advance write counter and flush after N writes.
		self._write_counter += 1
		if 0 == (self._write_counter % self._flush_after_writes):
			self.flush()

	def flush(self):
		''' Flush cached values to site data file. '''

		# First link to a temporary file.
		os.rename(self.site_file_path, self._site_tmp_file_path)
		# Dump to site file.
		self._dump(open(self.site_file_path, 'w'))
		# Remove temporary file.
		os.remove(self._site_tmp_file_path)

	def _dump(self, site_file):
		''' Dump site data to file. '''
		site_file.write(str(self._cache))

	def _read(self, site_data):
		''' Read site data from file. '''

		# This is hilariously unsafe.
		data = eval(site_data.read())
		for variable, value in data.iteritems():
			self.set_value(variable, value)

	def recover(self):
		''' Recover site from disk. '''

		# Recover from standard file.
		try:
			with open(self.site_file_path, 'r') as site_data:
				self._read(site_data)
				return

		except IOError:
			if os.path.isfile(self.site_file_path):
				raise IOError('Failed to access site data file {}'
						.format(self.site_file_path))

		# Recover from tmp file.
		try:
			with open(self._site_tmp_file_path, 'r') as site_tmp_data:
				self._read(site_tmp_data)

				# Try to save the standard site file.
				with open(self.site_file_path, 'w') as site_data:
					self._dump(site_data)
				# Unlink the temporary file.
				os.remove(self._site_tmp_file_path)
				return

		except IOError:
			if os.path.isfile(self._site_tmp_file_path):
				raise IOError('Failed to access site temporary data file {}'
						.format(self._site_tmp_file_path))

		# There is no site file. Initialize it.
		try:
			with open(self.site_file_path, 'w') as site_data:
				self._dump(site_data)
		except IOError:
			raise IOError('Failed to initialize site data file {}'
					.format(self.site_file_path))

	def multiversion_clone(self):
		''' Return a multiversion clone of the site with a read-only interface. '''
		return DatabaseManager.MultiversionClone(self, self._cache)

	def dump(self, variable=None):
		''' Dump database values. '''

		if variable is not None:
			return self.get_value(variable)
		else:
			return copy.deepcopy(self._cache)


class CommandProcessor(object):
	''' Database command processor. '''

	def __init__(self, site_data_map, data_path):
		'''
		Initialize database with sites.

		Parameters
		----------
		site_data_map : dict
			Dict of site indices to dict of site variables and default values.
		data_path : string
			Path to site data.
		'''

		# Initialize database sites.
		self._sites = [Site(index, data, data_path)
				for index, data in site_data_map.iteritems()]
		self._variables = sorted(list(set(variable
				for variable in it.chain(*[data.iterkeys()
					for data in site_data_map.itervalues()]))))

	def send_command(self, command):
		''' Execute commands using the available copies algorithm. '''
		pass

	def __str__(self):
		''' Represent the sites as a matrix. '''
		
		FIELD_WIDTH = 4

		out = StringIO.StringIO()
		variable_fmt_str = '{}' + ((' ' + '{}') * (len(self._variables) - 1))
		site_line_fmt_str = 'Site {{:02d}}: {}\n'.format(variable_fmt_str)

		field_fmt = '{{:{}d}}'.format(FIELD_WIDTH)
		not_present = (' ' * (FIELD_WIDTH - 1)) + 'X'
		for site in self._sites:
			site_dump = site.dump()
			variable_states = [ field_fmt.format(site_dump[variable])
					if variable in site_dump else not_present
					for variable in self._variables]
			out.write(site_line_fmt_str.format(site.index, *variable_states))

		out.seek(0)
		return out.read()


class TestFile(object):
	''' Load a database test file and read commands. '''


def setup_repcrec():
	''' Setup database according to RepCRec specification. '''
	
	site_data_map = dict((index, dict((variable, 10 * variable)
		for variable in it.ifilter(
			lambda x: (0 == (x & 1)) or (index == (1 + (x / 2))),
			range(1, 21))))
		for index in range(1, 11))

	return CommandProcessor(site_data_map, 'data')

