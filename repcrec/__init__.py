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
from repcrec.database_manager import DatabaseManager
from repcrec.lock_manager import LockManager
from repcrec.site import Site
from repcrec.transaction_manager import TransactionManager
from repcrec.util import delegator, parse_txid, check_args_len
import os
import itertools as it
import re

class TestFile(object):
	''' Load a database test file and read commands. '''

	@staticmethod
	def _parse_commands(line):
		''' Parse the command and its arguments. '''

		# Ignore comments.
		cmd_groups, _, _ = line.partition('//')
		commands = []

		for cmd_group in it.ifilter(
				lambda command: len(command) > 0,
				cmd_groups.split(';')):
			match = re.match('([a-zA-Z0-9]+)\(([^)]*)\)', cmd_group.strip())
			if match is None:
				raise ValueError('Failed parsing command {}'.format(cmd_group))
			else:
				cmd, args = match.groups()
				cmd = cmd.strip()
				args = tuple(
						arg for arg in
						it.imap(lambda arg: arg.strip(), args.split(','))
						if len(arg) > 0)
				commands.append((cmd, args))

		if len(commands) > 0:
			return commands
		else:
			return None

	def __init__(self, file_path):
		''' Initialize from path. '''

		self._file_path = os.path.abspath(file_path)
		if not os.path.isfile(self._file_path):
			raise ValueError(
					'Test file {} does not exist'.format(self._file_path))

		# Open and parse the file.
		standard_commands = True
		self._commands = []
		self._debug_commands = []
		with open(self._file_path, 'r') as test_data:
			for line_num, line in enumerate(test_data):
				# Transition to debug when line matches '---'.
				if line.strip() == '---':
					standard_commands = False
					continue
				try:
					data = self._parse_commands(line)
				except ValueError:
					raise ValueError(
							'Error parsing line {}: {}'.format(line_num, line))
				if data is not None:
					if standard_commands is True:
						self._commands.append(data)
					else:
						for cmd, args in data:
							if cmd not in self._DEBUG_CMD_DELEGATORS:
								raise ValueError(('Bad debug command '
									'on line {}: {}').format(line_num, line))
							else:
								args_checker = self._DEBUG_CMD_DELEGATORS[cmd][0]
								args = args_checker(self, cmd, args)
							self._debug_commands.append((cmd, args))

	def __iter__(self):
		''' Iterate over commands. '''
		return iter(self._commands)

	# Delegators for (ARGUMENT_CHECKING, EXECUTION).
	_DEBUG_CMD_DELEGATORS = {
			'assertCommitted':
			(delegator('_get_txid_arg'), delegator('_assert_committed')),
			'assertAborted':
			(delegator('_get_txid_arg'), delegator('_assert_aborted')),
			}

	@staticmethod
	def _get_txid_arg(cmd, args):
		''' Check that this is a single argument of the form T[0-9]+. '''
		check_args_len(cmd, args, 1)
		return (parse_txid(cmd, args, 0),)

	@staticmethod
	def _log_debug_assert(result, msg):
		''' Log debug assert result. '''

		if result is True:
			print 'debug SUCCESS : {}'.format(msg)
		else:
			print 'debug FAILURE : {}'.format(msg)

	@classmethod
	def _assert_ended(cls, args, status_name, target_status, commit_abort_log):
		''' Check that some transaction ended. '''

		check_txid = args[0]

		for txid, _, status in commit_abort_log:
			if txid is check_txid:
				cls._log_debug_assert(
						status is target_status,
						'expecting {} for T{}'.format(status_name, txid))
				return

		cls._log_debug_assert(
				False,
				'T{} not found in the log'.format(check_txid))

	@classmethod
	def _assert_committed(cls, args, commit_abort_log):
		''' Check that some transaction committed. '''
		cls._assert_ended(
				args, 'COMMITTED', TransactionManager.COMMITTED, commit_abort_log)

	@classmethod
	def _assert_aborted(cls, args, commit_abort_log):
		''' Check that some transaction aborted. '''
		cls._assert_ended(
				args, 'ABORTED', TransactionManager.ABORTED, commit_abort_log)

	def assert_debug_commands(self, commmit_abort_log):
		''' Check debug assertions made in the test file. '''

		for cmd, args in self._debug_commands:
			self._DEBUG_CMD_DELEGATORS[cmd][1](self, args, commmit_abort_log)


