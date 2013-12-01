'''
Tests for LockManager.
'''
import unittest
from repcrec import LockManager
import random

class LockManagerTest(unittest.TestCase):

	def setUp(self):
		''' Setup test. '''

		self._variables = range(100)
		random.shuffle(self._variables)
		self._variables = self._variables[:10]
		self._lock_manager = LockManager()

	def test_all_readers(self):
		'''
		Succeeds for readers when either unlocked or read-locked only.
		'''

		lock_manager, variables = self._lock_manager, self._variables

		# All readers should succeed. Locks are reentrant.
		locks = set()
		for _ in range(1000):
			# Any reader will succeed.
			reader = random.randint(1, 100)
			variable = variables[random.randint(1, len(variables)) - 1]
			self.assertTrue(
				lock_manager.try_lock(variable, reader, LockManager.R_LOCK))
			self.assertEqual(
					LockManager.R_LOCK,
					lock_manager.get_lock_state(variable, reader))
			locks.add((variable, reader))

			# A different writer cannot succeed.
			while True:
				writer = random.randint(1, 100)
				if writer != reader:
					break
			self.assertFalse(
				lock_manager.try_lock(variable, writer, LockManager.RW_LOCK))

		# Unlock all.
		for variable, txid in locks:
			lock_manager.unlock(variable, txid)
			self.assertEqual(
					None,
					lock_manager.get_lock_state(variable, txid))

	def test_all_writers(self):
		'''
		Succeeds for writers when either unlocked or read-locked exclusively by
		the calling transaction.
		'''

		lock_manager, variables = self._lock_manager, self._variables

		locks = set()

		# Read lock promoted to write lock should succeed.
		variable = variables[0]
		writer = random.randint(1, 100)
		self.assertTrue(
			lock_manager.try_lock(variable, writer, LockManager.R_LOCK))
		self.assertTrue(
			lock_manager.try_lock(variable, writer, LockManager.RW_LOCK))
		self.assertEqual(
				LockManager.RW_LOCK,
				lock_manager.get_lock_state(variable, writer))
		locks.add((variable, writer))

		for variable in variables[1:]:
			# Get a transaction and write lock it.
			writer = random.randint(1, 100)
			self.assertTrue(
				lock_manager.try_lock(variable, writer, LockManager.RW_LOCK))
			self.assertEqual(
					LockManager.RW_LOCK,
					lock_manager.get_lock_state(variable, writer))
			locks.add((variable, writer))

			# A different reader or writer cannot succeed.
			while True:
				other = random.randint(1, 100)
				if other != writer:
					break
			self.assertFalse(
				lock_manager.try_lock(variable, other, LockManager.RW_LOCK))
			self.assertFalse(
				lock_manager.try_lock(variable, other, LockManager.R_LOCK))

		# Unlock all.
		for variable, txid in locks:
			lock_manager.unlock(variable, txid)
			self.assertEqual(
					None,
					lock_manager.get_lock_state(variable, txid))


if __name__ == '__main__':
	unittest.main()
