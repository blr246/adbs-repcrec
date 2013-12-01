'''
Database lock manager.
'''

import StringIO

class LockManager(object):
	''' Database lock manager. '''

	_ALLOWED_MODES = range(3)
	_UNLOCKED, R_LOCK, RW_LOCK = _ALLOWED_MODES

	_LOCK_TABLE_STATES = {
			_UNLOCKED: 'U',
			R_LOCK: 'R',
			RW_LOCK: 'W',
			}

	def __init__(self):
		'''
		Initialize the lock manager. All variables are unlocked initially.
		'''
		self._lock_table = dict()

	def __repr__(self):
		def fmt_lock_state(txids, state):
			''' Format lock state string. '''
			if len(txids) > 0:
				return '{} <- {}'.format(
						txids, self._LOCK_TABLE_STATES[state])
			else:
				return self._LOCK_TABLE_STATES[self._UNLOCKED]

		out = StringIO.StringIO()
		for variable, (txids, state) in self._lock_table.iteritems():
			out.write('{:3d}: {}\n'.format(
				variable, fmt_lock_state(txids, state)))
		out.seek(0)
		return out.read()

	def get_locks(self, variable):
		'''
		Get the locks for a variable.

		Parameters
		----------
		variable : integer
			Name of variable for which to query locks.

		Returns
		-------
		lock_state : tuple of (txids, node) or None
			Ids of transactions holding a lock for the variable and the mode of
			the lock or None if no locks are held.
		'''

		if variable not in self._lock_table:
			return None
		else:
			return self._lock_table[variable]

	def try_lock(self, variable, txid, mode):
		'''
		Try to lock a variable for a given transaction and mode.

		Parameters
		----------
		variable : integer
			Variable to lock.
		txid : integer
			Transaction id to own the lock.
		mode : LockManager.R_LOCK or LockManager.RW_LOCK
			The lock type.

		Returns
		-------
		lock_state : boolean
			True if locked and False otherwise.
		'''

		if mode not in self._ALLOWED_MODES:
			raise ValueError(
					'Lock mode {} is not recognized'.format(mode))

		# Add a lock table entry for a new variable.
		if variable not in self._lock_table:
			self._lock_table[variable] = ([], self._UNLOCKED)

		# Lookup lock state.
		txids, state = self._lock_table[variable]

		if len(txids) is 0:
			# The lock is not claimed. Claim it.
			self._lock_table[variable] = ([txid], mode)
			return True

		elif txid in txids:
			# Either the txid has the desired lock type already, or it is
			# trying to promote.
			if mode is self.RW_LOCK:
				if len(txids) is 1:
					# Become the unique writer.
					self._lock_table[variable] = (txids, mode)
					return True
				else:
					# There are multiple read clients already.
					return False
			else:
				# No need to change the lock mode if we want to read.
				return True

		elif state is self.R_LOCK and mode is self.R_LOCK:
			# Add a new read lock client.
			txids.append(txid)
			return True

		else:
			# The write lock is held already.
			return False

	def get_lock_state(self, variable, txid):
		'''
		Check the lock type held by a transation.

		Parameters
		----------
		txid : integer
			Id of locking transation.
		variable : integer
			Id of variable.

		Returns
		-------
		lock_state : state or None
			None if not locked by txid otherwise either LockManager.R_LOCK or
			LockManager.RW_LOCK.
		'''

		if variable not in self._lock_table:
			return None

		txids, state = self._lock_table[variable]
		if txid in txids:
			return state
		else:
			return None

	def unlock(self, variable, txid):
		''' Unlock a variable. '''

		if variable not in self._lock_table:
			raise ValueError('Variable {} not locked at all'.format(variable))

		# Lookup lock state.
		txids, _ = self._lock_table[variable]

		# Must be locked by this transaction.
		if txid not in txids:
			raise ValueError(('Variable {} is not locked by '
				'transaction {}').format(variable, txid))

		txids.remove(txid)
		if len(txids) is 0:
			self._lock_table[variable] = ([], self._UNLOCKED)

	def unlock_all(self, txid):
		''' Batch unlock all locks held by the given transaction. '''

		for variable, (txids, _) in self._lock_table.iteritems():
			if txid in txids:
				self.unlock(variable, txid)


