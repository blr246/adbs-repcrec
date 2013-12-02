'''
Database site.
'''

from repcrec.lock_manager import LockManager
from repcrec.database_manager import DatabaseManager
from repcrec.util import OperationStatus

import collections

class Site(object):
	''' Represents a database site. '''

	class MultiversionClone(object):
		''' A multiversion read clone. '''

		def __init__(self, site, read_clone):
			self._database_manager = read_clone
			self._site = site
			self._variables = self._database_manager.variables

		@property
		def index(self):
			''' See Site.index. '''
			return self._site.index

		@property
		def up_since(self):
			''' See Site.up_since. '''
			return self._site.up_since

		def try_read(self, txid, variable):
			'''
			Try to read this site.

			Parameters
			----------
			txid : integer
				Transaction id.
			variable : integer
				Variable id to read.

			Returns
			-------
			status : OperationStatus or None
				None when the variable is not available and OperationStatus
				otherwise.
			'''

			# If the site is down, then return None.
			if self._site.up_since is None:
				return None

			# Otherwise we have a read if the variable is owned by this site.
			if variable not in self._variables:
				return None

			return OperationStatus(True, variable,
					self._database_manager.read(variable), None)

		def try_write(self, txid, variable, value):
			''' Read-only clones cannot write. '''
			raise RuntimeError(('Attempted write (x{}, {}) on site {} by T{} '
				'is not permitted; transaction is read-only').format(
						variable, value, self.index, txid))

		def abort(self, txid):
			''' Abort an open transaction. '''
			pass

		def commit(self, txid):
			''' Commit all pending writes for a transaction. '''
			pass


	def __init__(self, index, variable_defaults, owned_variables, data_path):
		'''
		Initialize the site.

		Parameters
		----------
		index : integer
			The integer index of the site.
		variable_defaults : dict
			Dict of variables replicated at this site and their default values.
		owned_variables : set of variables
			Set of variables that are owned exclusively by this site.
		data_path : string
			Path where site data file resides.
		'''

		self._variables = set(variable_defaults.iterkeys())
		self._database_manager = DatabaseManager(
				variable_defaults, data_path, 'site_{}'.format(index))
		self._lock_manager = LockManager()
		self._index = index
		self._up_since = 0
		self._available_variables = self._variables
		self._owned_variables = set(owned_variables)
		self._pending_writes = collections.defaultdict(list)

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

	@property
	def up_since(self):
		''' Time when site started or recovered. None if site is down. '''
		return self._up_since

	def mark_down(self):
		''' Mark the Site down. '''
		self._up_since = None
		self._available_variables = set()
		self._lock_manager = LockManager()

	def recover(self, tick):
		''' Recover downed site. '''
		if self._up_since is not None:
			raise ValueError('Site is not down to recover()')

		assert len(self._available_variables) is 0, \
				'Site was down with available variables'

		for variable in self._variables:
			assert self._lock_manager.get_locks(variable) is None, \
					'Site was down but there are locks'

		self._up_since = tick

	def abort(self, txid):
		''' Abort an open transaction. '''

		if txid in self._pending_writes:
			del self._pending_writes[txid]
		self._lock_manager.unlock_all(txid)

	def commit(self, txid):
		''' Commit all pending writes for a transaction. '''

		if txid in self._pending_writes:
			self._database_manager.batch_write(self._pending_writes[txid])
			del self._pending_writes[txid]
		self._lock_manager.unlock_all(txid)

	def try_read(self, txid, variable):
		'''
		Try to read this site.

		Parameters
		----------
		txid : integer
			Transaction id.
		variable : integer
			Variable id to read.

		Returns
		-------
		status : OperationStatus or None
			None when the variable is not available and OperationStatus otherwise.
		'''

		# Is the site down?
		if self._up_since is None:
			return None

		# Does this site have this variable at all?
		if variable not in self._variables:
			return None

		# Is this variable available for reading?
		if variable not in self._owned_variables and \
				variable not in self._available_variables:
			return None

		else:
			# Try to get a read lock.
			if self._lock_manager.try_lock(
					variable, txid, LockManager.R_LOCK) is True:

				value = None

				# The only time that there can be a pending write on this
				# variable is if the calling transaction holds the write lock.
				if txid in self._pending_writes:
					for pending_variable, pending_value \
							in self._pending_writes[txid]:
						if variable is pending_variable:
							value = pending_value
							break

				# The variable is not written by this transaction, so get it
				# from the database.
				if value is None:
					value = self._database_manager.read(variable)

				return OperationStatus(True, variable, value, None)

			else:
				return OperationStatus(
						False, variable, None,
						self._lock_manager.get_locks(variable)[0])

	def try_write(self, txid, variable, value):
		'''
		Try to read this site.

		Parameters
		----------
		txid : integer
			Transaction id.
		variable : integer
			Variable id to read.
		value : integer
			Value to write for the variable.

		Returns
		-------
		status : OperationStatus or None
			None when the variable is not available and OperationStatus otherwise.
		'''

		# Is the site down?
		if self._up_since is None:
			return None

		# Does this site have this variable at all?
		if variable not in self._variables:
			return None

		# Try to get a write lock.
		if self._lock_manager.try_lock(
				variable, txid, LockManager.RW_LOCK) is True:

			# The write is pending until commit().
			self._pending_writes[txid].append((variable, value))
			self._available_variables.add(variable)
			return OperationStatus(True, variable, value, None)

		else:
			return OperationStatus(
					False, variable, value,
					self._lock_manager.get_locks(variable)[0])

	def multiversion_clone(self):
		''' Create site clone. '''
		return self.MultiversionClone(
				self, self._database_manager.multiversion_clone())


