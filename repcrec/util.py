'''
Common utilities.
'''

import itertools as it

class WaitDie(object):
	''' State management for wait-die algorithm. '''

	def __init__(self, open_tx, tx_tick):
		''' Initialize with transaction tick. '''
		self._tx_tick = tx_tick
		self._open_tx = open_tx
		self._oldest_blocker = 1 << 31
		self._blocked_by = None

	def append_blockers(self, waits_for):
		''' Append blockers to this transaction. '''

		oldest_waits_for, txid = min(it.imap(
			lambda txid: (self._open_tx[txid].start_time, txid), waits_for))
		if oldest_waits_for < self._oldest_blocker:
			self._oldest_blocker = oldest_waits_for
			self._blocked_by = txid

	def should_die(self):
		'''
		Check if transaction should die. Younger transactions abort rather
		than wait for older ones.
		'''
		#print 'tx_tick {} > {} oldest_blocker'.format(
		#		self._tx_tick, self._oldest_blocker)
		return self._tx_tick > self._oldest_blocker

	@property
	def blocked_by(self):
		''' Return id of blocking transaction. '''
		return self._blocked_by


class TxRecord(object):
	''' Record tracking an in-progress transaction. '''

	def __init__(self, txid, start_time):
		self._txid = txid
		self._start_time = start_time
		self._sites_accessed = set()
		self._alive = True
		self._pending_commands = []
		self._ended = False

	@property
	def txid(self):
		''' Get transaction id. '''
		return self._txid

	@property
	def start_time(self):
		''' Get transaction start time. '''
		return self._start_time

	@property
	def sites_accessed(self):
		''' Get sites accessed by transaction. '''
		return self._sites_accessed

	@property
	def alive(self):
		''' Check if the transation is alive. '''
		return self._alive

	@property
	def ended(self):
		''' Check if the transaction is ended. '''
		return self._ended

	@property
	def pending(self):
		''' Check if there are pending commands. '''
		return len(self._pending_commands) is not 0

	def die(self):
		''' Mark that the transaction dead. '''
		self._alive = False

	def end(self):
		''' Mark that the transaction received an end command. '''
		if self._ended is True:
			raise ValueError(
					'T{} ended already'.format(self._txid))
		else:
			self._ended = False

	def peek_pending(self):
		''' Get next pending command. '''
		return self._pending_commands[0]

	def pop_pending(self):
		''' Pop next pending command. '''
		self._pending_commands = self._pending_commands[1:]

	def append_pending(self, cmd, args, runner):
		''' Append a pending command. '''
		self._pending_commands.append(((cmd, args), runner))

	def mark_site_accessed(self, site):
		''' Mark that transaction accessed a site. '''
		self._sites_accessed.add(site)


def delegator(method):
	''' Create a method delegator. '''

	def call_method(delegate, *args, **kwargs):
		''' Call method on delegate. '''
		func = getattr(delegate, method)
		return func(*args, **kwargs)

	return call_method

def format_command(cmd, args):
	''' Format command string. '''
	return '{}({})'.format(cmd, ', '.join(args))

def check_args_len(cmd, args, expect_len):
	''' Check command arguments length. '''

	if len(args) != expect_len:
		raise ValueError(('Command {} should have only {} '
			'argument(s)').format(format_command(cmd, args), expect_len))

def cmd_error(cmd, args, msg):
	''' Generate command error prefix. '''
	return 'ERROR CMD {} : {}'.format(format_command(cmd, args), msg)

def parse_id(cmd, args, idx, first_match, name):
	''' Parse transaction id of the form X[0-9]+. '''

	raw = args[idx]
	if raw[0] != first_match:
		raise ValueError(cmd_error(cmd, args,
			'{} {} must match {}[0-9]+'.format(name, raw, first_match)))

	parsed = int(raw[1:])
	if parsed < 0:
		raise ValueError('{} {} is invalid. Must be > 0'
				.format(name, parsed))

	return parsed

def parse_txid(cmd, args, idx):
	''' Parse transaction id. '''
	return parse_id(cmd, args, idx, 'T', 'Transaction id')

def parse_variable(cmd, args, idx):
	''' Parse transaction id. '''
	return parse_id(cmd, args, idx, 'x', 'Variable')
