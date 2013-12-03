'''
RepCRec command utilities. These classes and functions are used to read RepCRec
commands as strings and produce parsed tuples of (command (args, ...)) for
processing with TransactionManager.send_commands().

Beyond the standard RepCRec specification, we support the special debug
commands assertCommitted() and assertAborted() used to check the results of
test files.

(c) 2013 Brandon Reiss
'''
from repcrec import TransactionManager
from repcrec.util import delegator, parse_txid, check_args_len

import os
import itertools as it
import re

def parse_commands(line):
	'''
	Parse lines containing RepCRec commands into the format accepted by
	TransactionManager.send_commands().

	Parameters
	----------
	line : string
		A line from command input.

	Returns
	-------
	commands : list of commands
		List of tuples of the form (command, (args, ...)) ready for processing
		by TransactionManager.send_commands().
	'''

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

class CommandStreamReader(object):
	''' Read commands from a file stream. '''

	def __init__(self, stream):
		''' nitialize from a file stream.'''
		self._stream = stream

	def __iter__(self):
		'''
		Iterate over command stream returning (command, (args, ...) tuples.

		Note that this may be used only once since the underlying stream is
		spent in the process. An example usage is

			for commands in CommandStreamReader(sys.stdin):
				transaction_manager.send_commands(commands)

		where TransactionManager is an instance of repcrec.TransactionManager.
		'''

		for line in self._stream:
			commands = parse_commands(line)
			if commands is not None:
				yield commands

class TestFile(object):
	''' Load a database test file and read commands. '''

	def _parse(self):
		''' Parse the input file. '''

		# Open and parse the file.
		standard_commands = True
		with open(self._file_path, 'r') as test_data:
			for line_num, line in enumerate(test_data):
				# Transition to debug when line matches '---'.
				if line.strip() == '---':
					standard_commands = False
					continue
				try:
					data = parse_commands(line)
				except ValueError:
					raise ValueError(
							'Error parsing line {}: {}'.format(line_num, line))
				if data is None:
					continue

				# If we switched to debug commands, then verify them now.
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

	def __init__(self, file_path):
		''' Initialize from file path. '''

		if not os.path.isfile(file_path):
			raise ValueError(
					'Test file {} does not exist'.format(file_path))

		self._file_path = os.path.abspath(file_path)
		self._commands = []
		self._debug_commands = []

		self._parse()

	def __iter__(self):
		'''
		Iterate over commands. Clients may iterate over commands as many times
		as is needed since all commands are stored in memory within the
		TestFile instance.
		'''
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
		''' Log debug command assertXxx() result. '''

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
		'''
		Check debug assertions made in the test file.

		Parameters
		----------
		commmit_abort_log : iterable of tuples
			Iterable of (txid, tick, status) tuples where status is one of
			TransactionManager.COMMITTED or TransactionManager.ABORTED.
		'''

		for cmd, args in self._debug_commands:
			self._DEBUG_CMD_DELEGATORS[cmd][1](self, args, commmit_abort_log)


