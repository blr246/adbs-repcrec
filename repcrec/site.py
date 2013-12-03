'''
The database site has a LockManager and a DatabaseManager and executes
two-phase locked operations on data items. The site keeps track of variables
that are available for reading and when it last recovered.

Sites also support multiversion read clones. Note that the clones behave as
though they are copied locally to the caller through a client interface in that
a read clone taken for some site will still return data even when that site is
down. A site must be running only when the read clone is first requested. Since
read clones are reference counted, they must be released by calling either
commit() or abort() for each transaction that requested them.

(c) 2013 Brandon Reiss
'''

from repcrec.lock_manager import LockManager
from repcrec.database_manager import DatabaseManager
from repcrec.util import OperationStatus

import collections

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

	def _raise_ioerror_if_down(self):
		''' Raise IOError() when site is down. '''
		if self._up_since is None:
			raise IOError('Site {} is down'.format(self._index))

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
		self._pending_writes = collections.defaultdict(list)

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

	def _release_multiversion_clone(self, txid, tick):
		''' Release a reader on a multiversion clone. '''

		if tick not in self._multiversion_clones:
			raise ValueError(('Multiversion clone '
				'for T{} at time t{} is invalid').format(txid, tick))

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
		Abort an open transaction with zero side-effects on the site data.

		All locks held by the transaction will be freed. Releases a
		multiversion clone when tick is not None.
		'''

		# Read-only transactions are never down.
		if tick is None:
			self._raise_ioerror_if_down()

		if txid in self._pending_writes:
			del self._pending_writes[txid]
		self._lock_manager.unlock_all(txid)

		if tick is not None:
			self._release_multiversion_clone(txid, tick)

	def commit(self, txid, tick):
		'''
		Commit all pending writes for a transaction atomically.

		All locks held by the transaction will be freed. Releases a
		multiversion clone when tick is not None.
		'''

		# Read-only transactions are never down.
		if tick is None:
			self._raise_ioerror_if_down()

		if txid in self._pending_writes:
			self._database_manager.batch_write(self._pending_writes[txid])
			# Variables are written and so they are now available for reading.
			for variable, _ in self._pending_writes[txid]:
				self._available_variables.add(variable)
			del self._pending_writes[txid]
		self._lock_manager.unlock_all(txid)

		if tick is not None:
			self._release_multiversion_clone(txid, tick)

	def try_read(self, txid, variable, tick):
		'''
		Try to read from this site.

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
			self._raise_ioerror_if_down()

		# Does this site have this variable at all?
		if variable not in self._variables:
			return None

		# Is this a multiversion clone read? If so, it's "local".
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
			# Try to get a read lock. Since this is two-phase locking, we will
			# succeed if we can get a lock.
			if self._lock_manager.try_lock(
					variable, txid, LockManager.R_LOCK) is True:

				value = None

				# The only time that there can be a pending write on this
				# variable is if the calling transaction holds the write lock
				# since we were able to get a read lock.
				if txid in self._pending_writes:
					for pend_var, pend_val in self._pending_writes[txid]:
						if variable is pend_var:
							value = pend_val
							break

				# The variable is not written by this transaction, so get it
				# from the database.
				if value is None:
					value = self._database_manager.read(variable)

				return OperationStatus(True, variable, value, None)

			else:
				# No lock mean we return nothing.
				return OperationStatus(
						False, variable, None,
						self._lock_manager.get_locks(variable)[0])

	def try_write(self, txid, variable, value):
		'''
		Try to write to this site. All writes are pending until they are
		flushed atomically to the DatabaseManager on commit().

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

		# Sites must be up for writing. Multiversion clones do not write.
		self._raise_ioerror_if_down()

		# Does this site have this variable at all?
		if variable not in self._variables:
			return None

		# Try to get a write lock.
		if self._lock_manager.try_lock(
				variable, txid, LockManager.RW_LOCK) is True:

			# The write is pending until commit().
			self._pending_writes[txid].append((variable, value))
			return OperationStatus(True, variable, value, None)

		else:
			# No lock means we can't submit this write here.
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
		transparently by the site connection client, but this is inefficient
		since all site data must be cloned over the network.

		Note that commit() or abort() must be called once for each call to
		multiversion_clone() in order to release the clone.

		Beware that the up_since and is_up() attributes are ambiguous for
		read-only states.
		'''

		self._raise_ioerror_if_down()

		# Initialize clone or increase use count.
		if tick not in self._multiversion_clones:
			self._multiversion_clones[tick] = \
					(1, self._database_manager.multiversion_clone())
		else:
			clone = self._multiversion_clones[tick]
			self._multiversion_clones[tick] = (clone[0] + 1, clone[1])


