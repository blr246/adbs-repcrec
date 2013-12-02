'''
Database site.
'''

from repcrec.lock_manager import LockManager
from repcrec.database_manager import DatabaseManager
from repcrec.util import OperationStatus

import collections

def _raise_ioerror_if_down(index, up_since):
	''' Raise IOError() when down. '''
	if up_since is None:
		raise IOError('Site {} is down'.format(index))

class Site(object):
	''' Represents a database site. '''

	def __init__(self, index, variable_defaults, owned_variables, tick, data_path):
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
		tick : integer
			Time that site is first starting.
		data_path : string
			Path where site data file resides.
		'''

		self._index = index
		self._variables = set(variable_defaults.iterkeys())
		self._up_since = tick
		self._available_variables = self._variables
		self._owned_variables = set(owned_variables)

		self._database_manager = DatabaseManager(
				variable_defaults, data_path, 'site_{}'.format(index))
		self._lock_manager = LockManager()

		self._pending_writes = collections.defaultdict(list)
		self._multiversion_clones = dict()

	def __repr__(self):
		return '{{ \'index\': {}, \'data\': {}, \'locks\': {} }}'.format(
				self._index, self._database_manager, self._lock_manager)

	def dump(self):
		'''
		Dump committed site data. This is a debug API.

		Returns
		-------
		values : dict
			Map from variable to value.
		available : dict
			Map from variable to True if available to read and False otherwise.
		'''

		available = lambda variable: \
				variable in self._owned_variables or \
				variable in self._available_variables

		return self._database_manager.dump(), \
				dict((variable, available(variable))
						for variable in self._variables)

	@property
	def index(self):
		''' The site index. '''
		return self._index

	@property
	def up_since(self):
		''' Time when site started or recovered. None if site is down. '''
		return self._up_since

	def is_up(self):
		''' Query whether wite is up. '''
		return self._up_since is not None

	def fail(self):
		''' Fail the site. '''
		self._up_since = None
		self._available_variables = set()
		self._lock_manager = LockManager()

	def recover(self, tick):
		''' Recover downed site. '''

		if self._up_since is not None:
			raise ValueError('Site is not down to recover()')

		assert len(self._available_variables) is 0, \
				'Site was down with available variables'

		assert all(self._lock_manager.get_locks(variable) is None
				for variable in self._variables), \
						'Site was down but there are locks'

		self._up_since = tick

	def _release_multiversion_clone(self, tick):
		''' Release a reader on a multiversion clone. '''

		use_count, clone = self._multiversion_clones[tick]
		use_count -= 1

		if tick not in self._multiversion_clones:
			raise ValueError(('Multiversion clone {} '
				'does not exist').format(tick))

		if use_count is 0:
			del self._multiversion_clones[tick]
		else:
			self._multiversion_clones[tick] = (use_count, clone)

		return use_count

	def abort(self, txid, tick):
		'''
		Abort an open transaction. Release multiversion clone when tick is not
		None.
		'''

		# Read-only transactions are never down.
		if tick is None:
			_raise_ioerror_if_down(self._index, self._up_since)

		if txid in self._pending_writes:
			del self._pending_writes[txid]
		self._lock_manager.unlock_all(txid)

		if tick is not None and tick in self._multiversion_clones:
			self._release_multiversion_clone(tick)

	def commit(self, txid, tick):
		'''
		Commit all pending writes for a transaction. Release multiversion clone
		when tick is not None.
		'''

		# Read-only transactions are never down.
		if tick is None:
			_raise_ioerror_if_down(self._index, self._up_since)

		if txid in self._pending_writes:
			self._database_manager.batch_write(self._pending_writes[txid])
			del self._pending_writes[txid]
		self._lock_manager.unlock_all(txid)

		if tick is not None and tick in self._multiversion_clones:
			self._release_multiversion_clone(tick)

	def try_read(self, txid, variable, tick):
		'''
		Try to read this site.

		Parameters
		----------
		txid : integer
			Transaction id.
		variable : integer
			Variable id to read.
		tick : integer or None
			Token used to access multiversion clone or None.

		Returns
		-------
		status : OperationStatus or None
			None when the variable is not available and OperationStatus otherwise.
		'''

		# Read-only transactions are never down.
		if tick is None:
			_raise_ioerror_if_down(self._index, self._up_since)

		# Does this site have this variable at all?
		if variable not in self._variables:
			return None

		# Is this a multiversion clone read?
		if tick is not None:
			if tick in self._multiversion_clones:
				value = self._multiversion_clones[tick][1].read(variable)
				return OperationStatus(True, variable, value, None)
			else:
				raise ValueError(('Multiversion clone {} '
						'does not exist').format(tick))

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

		_raise_ioerror_if_down(self._index, self._up_since)

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

	def multiversion_clone(self, tick):
		'''
		Create site clone at the given tick. The tick will be used as a
		token to access this clone from try_read().

		All multiversion clones behave as though they are local to the caller.
		In other words, after their creation the downed status of the site is
		ignored and all operations will succeed unconditionally. In a real
		distributed system, such functionality could be implemented
		transparently by the site connection client.

		Note that commit() or abort() must be called once for each call to
		multiversion_clone() in order to release the clone.

		Beware that the up_since and is_up() attributes are ambiguous for
		read-only states.
		'''

		_raise_ioerror_if_down(self._index, self._up_since)

		# Initialize clone or increase use count.
		if tick not in self._multiversion_clones:
			self._multiversion_clones[tick] = \
					(1, self._database_manager.multiversion_clone())
		else:
			clone = self._multiversion_clones[tick]
			self._multiversion_clones[tick] = (clone[0] + 1, clone[1])


