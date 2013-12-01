'''
Database command processor.
'''
from repcrec.site import Site
from repcrec.util import delegator
from repcrec.util import \
		WaitDie, TxRecord, parse_variable, parse_txid, check_args_len, \
		cmd_error, format_command
import itertools as it
import StringIO
import collections

class TransactionManager(object):
	''' Database command processor. '''

	COMMITTED, ABORTED = range(2)

	def __init__(self, data_file_map, data_path):
		'''
		Initialize database with sites.

		Parameters
		----------
		data_file_map : dict
			Dict of site indices to dict of site variables and default values.
		data_path : string
			Path to site data.
		'''

		# Discover owned variables by first getting map of { var : [sites] }
		# and then getting map of { site : [owned vars] }.
		var_to_site = reduce(lambda var_to_site, (index, var_dict):
				reduce(lambda var_to_site, var:
					var_to_site[var].append(index) or var_to_site,
					var_dict.iterkeys(), var_to_site),
				data_file_map.iteritems(), collections.defaultdict(list))
		site_owned_vars = reduce(lambda site_owned_vars, (var, sites):
				site_owned_vars if len(sites) > 1 else
				site_owned_vars[sites[0]].append(var) or site_owned_vars,
				var_to_site.iteritems(), collections.defaultdict(list))

		# Initialize database sites.
		self._sites = [Site(index, data, site_owned_vars[index], data_path)
				for index, data in data_file_map.iteritems()]

		# Get sorted union of variables across all sites.
		self._variables = sorted(list(set(variable
				for variable in it.chain(*[data.iterkeys()
					for data in data_file_map.itervalues()]))))

		# Track open transactions, timing, and log commits and aborts.
		self._open_tx = dict()
		self._tick = 0
		self._commit_abort_log = []

	def _log_at_time(self, txid, msg):
		''' Log a message with timestamp. '''
		if txid is not None:
			print '{:<4s} {:>4s} : {}'.format(
					't{},'.format(self._tick),
					'T{}'.format(txid), msg)
		else:
			print '{:<4s} {:>4s} : {}'.format(
					't{},'.format(self._tick),
					'--', msg)

	def _begin(self, cmd, args):
		''' Begin a transaction. This command does not block. '''

		check_args_len(cmd, args, 1)

		txid = parse_txid(cmd, args, 0)

		if txid in self._open_tx:
			raise ValueError(cmd_error(cmd, args,
				'Cannot begin T{}; already started'.format(txid)))
		else:
			self._open_tx[txid] = TxRecord(txid, self._tick)
			self._log_at_time(txid, 'started')

	def _append_end(self, cmd, args):
		''' Receive the command to end a transaction. '''

		check_args_len(cmd, args, 1)

		txid = parse_txid(cmd, args, 0)

		if txid not in self._open_tx:
			raise ValueError(cmd_error(cmd, args,
				'Cannot end T{}; not started'.format(txid)))

		transaction = self._open_tx[txid]

		transaction.end()
		transaction.append_pending(cmd, args,
				self._runner(self._end, (transaction,)))

	def _end(self, transaction):
		'''
		End a transaction. Ensures that all sites are up for the duration of
		the transaction.
		'''

		# TODO: read-only transactions do not need to abort.

		del self._open_tx[transaction.txid]

		abort = lambda site: site.abort(transaction.txid)
		commit = lambda site: site.commit(transaction.txid)

		if transaction.alive is True:
			# Check that all sites are up since the transaction started.
			action = commit
			for site in it.ifilter(
					lambda site: site.index in transaction.sites_accessed,
					self._sites):
				# Abort if any site accessed went down before now.
				if site.up_since > transaction.start_time:
					action = abort
					break
		else:
			action = abort

		for site in self._sites:
			action(site)

		self._log_at_time(transaction.txid,
				'committed' if action is commit else 'aborted')

		self._commit_abort_log.append((
			transaction.txid,
			transaction.start_time,
			self.COMMITTED if action is commit else self.ABORTED
			))

		return True

	def _append_read(self, cmd, args):
		''' Receive read command for a transaction. '''

		check_args_len(cmd, args, 2)

		txid = parse_txid(cmd, args, 0)
		if txid not in self._open_tx:
			raise ValueError(cmd_error(cmd, args,
				'T{} is not active'.format(txid)))
		transaction = self._open_tx[txid]

		variable = parse_variable(cmd, args, 1)
		if variable not in self._variables:
			raise ValueError(cmd_error(cmd, args,
				'Variable {} is not in the database'.format(txid)))

		transaction.append_pending(cmd, args,
				self._runner(self._read, (transaction, variable)))

	def _read(self, transaction, variable):
		'''
		Read a variable for a transaction. Uses the wait-die algorithm to
		decide whether or not to block a transaction.
		'''

		# See if the transaction is not alive.
		if transaction.alive is False:
			self._log_at_time(transaction.txid,
					'ignoring read x{}'.format(variable))
			return True

		# Locate an eligible site to read.
		wait_die = WaitDie(self._open_tx, transaction.start_time)
		responses = 0
		for site in self._sites:
			read_status = site.try_read(transaction.txid, variable)

			# Ignore sites that don't manage the variable.
			if read_status is None:
				continue
			responses += 1

			if read_status.success is True:
				self._log_at_time(transaction.txid,
						'read x{} -> {} from site {}'.format(
							variable, read_status.value, site.index))
				return True

			else:
				wait_die.append_blockers(read_status.waits_for)

		if responses is 0:
			self._log_at_time(transaction.txid,
					'waiting to read x{}; no available sites'.format(variable))
			return False

		# See if we should block or die.
		if wait_die.should_die():
			transaction.die()
			self._log_at_time(transaction.txid, 'killing by wait-die')
			self._end(transaction)
			return True
		else:
			self._log_at_time(transaction.txid,
					'blocked by T{} reading x{}'.format(
						wait_die.blocked_by, variable))
			return False

	def _append_write(self, cmd, args):
		''' Receive write command for a transaction. '''

		check_args_len(cmd, args, 3)

		txid = parse_txid(cmd, args, 0)
		if txid not in self._open_tx:
			raise ValueError(cmd_error(cmd, args,
				'T{} is not active'.format(txid)))
		transaction = self._open_tx[txid]

		variable = parse_variable(cmd, args, 1)
		if variable not in self._variables:
			raise ValueError(cmd_error(cmd, args,
				'Variable {} is not in the database'.format(txid)))

		value = int(args[2])

		transaction.append_pending(cmd, args,
				self._runner(self._write, (transaction, variable, value)))

	def _write(self, transaction, variable, value):
		'''
		Write a variable for a transaction. Uses the wait-die algorithm to
		decide whether or not to block a transaction.
		'''

		# See if the transaction is not alive.
		if transaction.alive is False:
			self._log_at_time(transaction.txid,
					'ignoring write (x{}, {})'.format(variable, value))
			return True

		wait_die = WaitDie(self._open_tx, transaction.start_time)
		sites_written = set()
		blocked = False
		for site in self._sites:
			write_status = site.try_write(transaction.txid, variable, value)

			# Ignore sites that don't manage the variable.
			if write_status is None:
				continue

			if write_status.success is True:
				transaction.mark_site_accessed(site.index)
				sites_written.add(site.index)
			else:
				blocked = True
				wait_die.append_blockers(write_status.waits_for)

		# Either we wrote no sites, some sites, or all available sites.
		if blocked is True:
			if wait_die.should_die():
				transaction.die()
				self._log_at_time(transaction.txid, 'killing by wait-die')
				self._end(transaction)
				return True
			else:
				self._log_at_time(transaction.txid,
						'blocked by T{} writing x{}'.format(
							wait_die.blocked_by, variable))
				return False

		# Here we don't need to block so long as we wrote at least 1 site.
		elif len(sites_written) > 0:
			self._log_at_time(transaction.txid,
					'write x{} <- {} to sites {{{}}}'.format(
						variable, value,
						', '.join(it.imap(str, sites_written))))
			return True

		# Here we wrote 0 sites, so we need to wait.
		else:
			self._log_at_time(transaction.txid,
					'waiting to write (x{}, {}); no available sites'.format(
						variable, value))
			return False

	def _fail(self, cmd, args):
		''' Fail site. '''
		check_args_len(cmd, args, 1)

		# Find the site. Will return early.
		index = int(args[0])
		for site in self._sites:
			if site.index is index:
				site.mark_down()
				self._log_at_time(None, 'site {} is down'.format(index))
				return True

		raise ValueError(cmd_error(cmd, args,
			'Site {} does not exist'.format(index)))

	def _recover(self, cmd, args):
		''' Recover site. '''

		check_args_len(cmd, args, 1)

		# Find the site. Will return early.
		index = int(args[0])
		for site in self._sites:
			if site.index is index:
				site.recover(self._tick)
				self._log_at_time(None, 'site {} is up'.format(index))
				return True

		raise ValueError(cmd_error(cmd, args,
			'Site {} does not exist'.format(index)))

	def _dump(self, cmd, args):
		''' Dump database state. '''
		check_args_len(cmd, args, 0)
		self._log_at_time(None, 'dumping sites')
		print self

	@staticmethod
	def _run_pending(transaction):
		''' Run pending commands and return number that were run. '''

		# Flush all commands that are possible to execute.
		num_run = 0
		while transaction.pending:
			_, runner = transaction.peek_pending()
			if runner() is True:
				transaction.pop_pending()
				num_run += 1
			else:
				break
		return num_run

	@staticmethod
	def _runner(func, args):
		''' Command function closure. '''

		def closure():
			''' The function closure. '''
			return func(*args)

		return closure


	# Map of command names to their function delegates.
	_COMMAND_DELEGATORS = {
			'begin': delegator('_begin'),
			'end': delegator('_append_end'),
			'r': delegator('_append_read'),
			'w': delegator('_append_write'),
			'fail': delegator('_fail'),
			'recover': delegator('_recover'),
			'dump': delegator('_dump'),
			}

	def send_commands(self, commands):
		''' Advance tick and execute commands. '''

		self._tick += 1

		self._log_at_time(None, 'sending commands {}'.format(commands))

		for cmd, args in commands:
			cmd_lower = cmd.lower()

			# Send commands to their delegates using function callbacks.
			if cmd_lower in self._COMMAND_DELEGATORS:
				self._COMMAND_DELEGATORS[cmd_lower](self, cmd, args)
			else:
				raise ValueError('Command {} is not recognized'
					.format(format_command(cmd, args)))

		# Try to run all pending commands until there is no more progress.
		for transaction in self._open_tx.values():
			self._run_pending(transaction)

	def get_commit_abort_log(self):
		'''
		Get TransactionManager commit and abort log. Entries are of the form
			(TXID, TICK_END, STATUS)
		where status is one of TransactionManager.COMMITTED or
		TransactionManager.ABORTED.
		'''
		return tuple(self._commit_abort_log)

	# Field width used by __str__() method.
	_FIELD_WIDTH = 4

	def __str__(self):
		''' Represent the sites as a matrix. '''

		out = StringIO.StringIO()
		variable_fmt_str = '{}' + ('{}' * (len(self._variables) - 1))
		site_line_fmt_str = '{{:>3s}}:{}\n'.format(variable_fmt_str)

		out.write('    ')
		for variable in self._variables:
			out.write(('{{:>{}}}'.format(self._FIELD_WIDTH))
					.format('x{}'.format(variable)))
		out.write('\n')

		field_fmt = '{{:{}d}}'.format(self._FIELD_WIDTH)
		not_present = '{{:>{}}}'.format(self._FIELD_WIDTH).format('-')
		for site in self._sites:
			site_dump = site.dump()
			variable_states = [ field_fmt.format(site_dump[variable])
					if variable in site_dump else not_present
					for variable in self._variables]
			out.write(site_line_fmt_str.format(
				'S{}'.format(site.index), *variable_states))

		out.seek(0)
		return out.read()


