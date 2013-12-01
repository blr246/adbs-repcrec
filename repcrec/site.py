'''
Database site.
'''

from repcrec.lock_manager import LockManager
from repcrec.database_manager import DatabaseManager

import collections

class Site(object):
	''' Represents a database site. '''

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
		self._down = False
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
		if self._down is not True:
			return self._up_since
		else:
			return None

	def mark_down(self):
		''' Mark the Site down. '''
		self._down = True
		self._available_variables = set()

	def recover(self, tick):
		''' Recover downed site. '''
		if self._down is not True:
			raise ValueError('Site is not down to recover()')

		assert len(self._available_variables) is 0, \
				'Site was down with available variables'

		self._down = False
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

	class OperationStatus(object):
		''' Operation status. '''

		def __init__(self, success, variable, value, waits_for):
			''' Initialize all data. '''

			assert ((success is True
				and variable is not None
				and waits_for is None)
				or (success is False
					and variable is not None
					and waits_for is not None)), 'Invalid status'

			self._success = success
			self._variable = variable
			self._value = value
			self._waits_for = waits_for

		@property
		def success(self):
			''' True when the operation succeeded and False otherwise. '''
			return self._success

		@property
		def variable(self):
			''' Variable accessed by operation. '''
			return self._variable

		@property
		def value(self):
			''' Value accessed by the operation. '''
			return self._value

		@property
		def waits_for(self):
			'''
			Iterable of transaction ids blocking the operation when success is
			False and None otherwise.
			'''
			return self._waits_for


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
		if self._down is True:
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

				return self.OperationStatus(True, variable, value, None)

			else:
				return self.OperationStatus(
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
		if self._down is True:
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
			return self.OperationStatus(True, variable, value, None)

		else:
			return self.OperationStatus(
					False, variable, value,
					self._lock_manager.get_locks(variable)[0])


