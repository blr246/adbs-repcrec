'''
The transaction manager is the database command processor. It takes a
serialized stream of incoming commands from multiple database clients and
submits operations to database sites in such a manner as to avoid deadlocks and
keep the database in a consistent state.

This transaction manager uses wait-die for conflict resolution and the
available copies algorithm for replication.

(c) 2013 Brandon Reiss
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
	''' Database transaction manager. '''

	COMMITTED, ABORTED = range(2)
	def __init__(self, data_file_map, data_path):
		'''
		Initialize the database with sites.

		Parameters
		----------
		data_file_map : dict
			Data are a dict of site indices to dict of site variables and
			default values.

			For instance,
			    data_file_map={ 1: { 5: 50 } }
			means that site 1 has variable 5 with default value 50.
		data_path : string
			Path to site data.
		'''

		# Track open transactions, timing, and log commits and aborts.
		self._open_tx = dict()
		self._blocked_queue = []
		self._commit_abort_log = []
		self._tick = 0

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
		make_site = lambda index, data: \
				Site(index, data,
						site_owned_vars[index], self._tick, data_path)
		self._sites = [make_site(index, data)
			for index, data in data_file_map.iteritems()]

		# Get sorted union of variables across all sites.
		self._variables = sorted(list(set(variable
				for variable in it.chain(*[data.iterkeys()
					for data in data_file_map.itervalues()]))))

	def _log_at_time(self, txid, msg):
		''' Log a message with timestamp for the given txid. '''

		if txid is not None:
			print '{:<4s} {:>4s} : {}'.format(
					't{},'.format(self._tick),
					'T{}'.format(txid), msg)
		else:
			print '{:<4s} {:>4s} : {}'.format(
					't{},'.format(self._tick),
					'--', msg)

	def _fail_if_blocked(self, cmd, args, transaction):
		''' Check that transaction is not blocked. '''

		if transaction in self._blocked_queue:
			(blk_cmd, blk_args), _ = transaction.blocked()
			raise RuntimeError(('Transaction {} '
				'is blocked by {} but received command {}').format(
					transaction.txid,
					format_command(blk_cmd, blk_args),
					format_command(cmd, args)))

	def _block(self, cmd, args, transaction, runner):
		''' Block the given transaction and add it to the queue. '''

		transaction.block(cmd, args, runner)
		if transaction not in self._blocked_queue:
			self._blocked_queue.append(transaction)

	@staticmethod
	def _wait_die_reason(variable, wait_die, transaction):
		''' Assemble wait-die reason string. '''
		return ('killing by wait-die reading x{}; '
				'(T{}, t{}) < (T{}, t{})').format(
						variable,
						wait_die.blocked_by, wait_die.blocked_by_age,
						transaction.txid, transaction.start_time)

	def _begin(self, cmd, args, is_ro=False):
		'''
		Begin a transaction. This command does not block.

		When is_ro is True, the transaction uses a set of multiversion clones
		for all sites that are up when this call executes. Each transaction
		keeps a record of the sites that it may access.
		'''

		check_args_len(cmd, args, 1)

		# Do not allow duplicate transactions.
		txid = parse_txid(cmd, args, 0)
		if txid in self._open_tx:
			raise ValueError(cmd_error(cmd, args,
				'Cannot begin T{}; already started'.format(txid)))

		# Spawn the transaction.
		if is_ro is False:
			self._open_tx[txid] = TxRecord(
					txid, self._tick, self._sites, None)
			self._log_at_time(txid, 'started')

		else:
			# Clone all running sites for read-only transactions.
			sites = [site for site in self._sites if site.is_up()]
			for site in sites:
				site.multiversion_clone(self._tick)
			# Add this transaction.
			self._open_tx[txid] = TxRecord(
					txid, self._tick, sites, self._tick)
			self._log_at_time(txid, 'started (read-only)')

	def _beginro(self, cmd, args):
		'''
		Begin a read-only transaction. See _begin() for more information.
		'''
		self._begin(cmd, args, is_ro=True)

	def _append_end(self, cmd, args):
		''' Receive and enqueue the command to end a transaction. '''

		check_args_len(cmd, args, 1)

		txid = parse_txid(cmd, args, 0)
		if txid not in self._open_tx:
			raise ValueError(cmd_error(cmd, args,
				'Cannot end T{}; not started'.format(txid)))

		transaction = self._open_tx[txid]
		self._fail_if_blocked(cmd, args, transaction)

		transaction.end()
		if self._end(transaction) is not True:
			self._block(cmd, args, transaction,
					self._runner(self._end, (transaction,)))

	def _end(self, transaction):
		'''
		End a transaction. Ensures that all sites are up since they were first
		accessed by the transaction.
		'''

		del self._open_tx[transaction.txid]

		# Actions for commit and abort.
		ro_token = transaction.start_time if transaction.is_read_only else None
		abort = lambda site: site.abort(transaction.txid, ro_token)
		commit = lambda site: site.commit(transaction.txid, ro_token)

		# Only alive transactions can commit.
		if transaction.alive is True:
			action = commit

			# Read-only transactions can skip the site accessed checks.
			if not transaction.is_read_only:

				# Extract (site, accessed_at_tick) tuple from site.
				site_accessed_tuple = lambda site: \
						(site, transaction.site_accessed_at(site.index))

				# Check that sites accessed are up since the time of first access.
				for site, accessed_at_tick in it.ifilter(
						lambda (_, tick): tick is not None,
						it.imap(site_accessed_tuple, transaction.sites)):

					# Abort if the site is down since it can only come up and
					# cause an abort.
					if site.is_up() is not True:
						self._log_at_time(transaction.txid,
								('aborting; accessed site {} '
									'is down').format(site.index))
						action = abort
						break

					# Abort if any site went down since the first access.
					if site.up_since > accessed_at_tick:
						self._log_at_time(transaction.txid,
								('aborting; site {} went down '
									'after first access').format(site.index))
						action = abort
						break

		else:
			action = abort

		# Apply action to all running sites.
		for site in it.ifilter(lambda site: site.is_up(), transaction.sites):
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
		''' Receive and enqueue a read command for a transaction. '''

		check_args_len(cmd, args, 2)

		txid = parse_txid(cmd, args, 0)
		if txid not in self._open_tx:
			raise ValueError(cmd_error(cmd, args,
				'T{} is not active'.format(txid)))

		transaction = self._open_tx[txid]
		self._fail_if_blocked(cmd, args, transaction)

		variable = parse_variable(cmd, args, 1)
		if variable not in self._variables:
			raise ValueError(cmd_error(cmd, args,
				'Variable {} is not in the database'.format(variable)))

		if self._read(transaction, variable) is not True:
			self._block(cmd, args, transaction,
				self._runner(self._read, (transaction, variable)))

	def _read(self, transaction, variable):
		'''
		Read a variable for a transaction from any available site. Uses the
		wait-die algorithm to decide whether or not to block a transaction.

		Read-only transactions will always succeed here so long as there was a
		site up when the transaction started that hosts the variable to read.
		'''

		if transaction.txid not in self._open_tx:
			raise RuntimeError('T{} is not open'.format(transaction.txid))

		# See if the transaction is not alive.
		if transaction.alive is False:
			self._log_at_time(transaction.txid,
					'ignoring read x{}'.format(variable))
			return True

		# Locate an eligible site to read.
		ro_token = transaction.start_time if transaction.is_read_only else None
		wait_die = WaitDie(self._open_tx, transaction.start_time)
		blocked, num_down = False, 0
		for site in transaction.sites:
			try:
				read_status = site.try_read(
						transaction.txid, variable, ro_token)

				# Ignore sites that don't manage the variable.
				if read_status is None:
					continue

				elif read_status.success is True:
					transaction.mark_site_accessed(site.index, self._tick)
					msg = 'read x{} -> {} from site {}'.format(
							variable, read_status.value, site.index)
					if transaction.is_read_only:
						msg += ' multiversion clone at t{}'.format(ro_token)
					self._log_at_time(transaction.txid, msg)
					return True

				else:
					blocked = True
					wait_die.append_blockers(read_status.waits_for)

			except IOError:
				# Keep track of downed sites since we need to query all sites
				# before rejecting a read as failed.
				num_down += 1

		status, should_die, reason = None, None, None

		# When blocked we check on wait-die for the result.
		if blocked is True:
			# See if we should block or die.
			if wait_die.should_die():
				should_die = True
				reason = self._wait_die_reason(variable, wait_die, transaction)
			else:
				status = False
				reason = 'blocked by T{} reading x{}'.format(
						wait_die.blocked_by, variable)

		# See if we have any downed sites. We can't reject an operation unless
		# we have tried all available sites.
		elif num_down > 0:
			status = False
			reason = 'waiting to read x{}; no available sites'.format(variable)

		# We read every site and the variable is not here.
		else:
			should_die = True
			site_indices = '{{{}}}'.format(
					', '.join(str(site.index) for site in transaction.sites))
			reason = 'killing; variable x{} not available on sites {}'.format(
					variable, site_indices)

		# Either we have (status, reason) or (should_die, reason).
		assert ((status is None) ^ (should_die is None)) \
				and reason is not None, 'Invalid status, should_die, reason'

		# Perform final steps for read.
		self._log_at_time(transaction.txid, reason)
		if should_die is True:
			transaction.die()
			self._end(transaction)
			return True
		else:
			return status

	def _append_write(self, cmd, args):
		''' Receive and enqueue a write command for a transaction. '''

		check_args_len(cmd, args, 3)

		txid = parse_txid(cmd, args, 0)
		if txid not in self._open_tx:
			raise ValueError(cmd_error(cmd, args,
				'T{} is not active'.format(txid)))

		transaction = self._open_tx[txid]
		self._fail_if_blocked(cmd, args, transaction)

		variable = parse_variable(cmd, args, 1)
		if variable not in self._variables:
			raise ValueError(cmd_error(cmd, args,
				'Variable {} is not in the database'.format(txid)))

		value = int(args[2])

		if self._write(transaction, variable, value) is not True:
			self._block(cmd, args, transaction,
					self._runner(self._write, (transaction, variable, value)))

	def _write(self, transaction, variable, value):
		'''
		Write a variable for a transaction to all available sites. Uses the
		wait-die algorithm to decide whether or not to block a transaction.
		'''

		if transaction.txid not in self._open_tx:
			raise RuntimeError('T{} is not open'.format(transaction.txid))

		# See if the transaction is not alive.
		if transaction.alive is False:
			self._log_at_time(transaction.txid,
					'ignoring write (x{}, {})'.format(variable, value))
			return True

		wait_die = WaitDie(self._open_tx, transaction.start_time)
		sites_written = set()
		blocked = False
		for site in transaction.sites:
			try:
				write_status = site.try_write(transaction.txid, variable, value)

				# Ignore sites that don't manage the variable.
				if write_status is None:
					continue

				elif write_status.success is True:
					transaction.mark_site_accessed(site.index, self._tick)
					sites_written.add(site.index)

				else:
					# The writes that succeeded so far will be retried later, but
					# this transaction holds the lock so it does not matter.
					blocked = True
					wait_die.append_blockers(write_status.waits_for)

			except IOError:
				# We don't need to track downed sites since we care only about
				# writing at least one copy.
				pass

		# Either we wrote no sites, some sites, or all available sites.
		status, should_die, reason = None, None, None

		# When blocked we check on wait-die for the result.
		if blocked is True:
			if wait_die.should_die():
				should_die = True
				reason = self._wait_die_reason(variable, wait_die, transaction)
			else:
				status = False
				reason = 'blocked by T{} writing x{}'.format(
						wait_die.blocked_by, variable)

		# Here we don't need to block so long as we wrote at least 1 site.
		elif len(sites_written) > 0:
			status = True
			reason = 'write x{} <- {} to sites {{{}}}'.format(
						variable, value,
						', '.join(it.imap(str, sites_written)))

		# Here we wrote 0 sites, so we need to wait.
		else:
			status = False
			reason = 'waiting to write (x{}, {}); no available sites'.format(
						variable, value)

		# Either we have (status, reason) or (should_die, reason).
		assert ((status is None) ^ (should_die is None)) \
				and reason is not None, 'Invalid status, should_die, reason'

		# Perform final steps.
		self._log_at_time(transaction.txid, reason)
		if should_die is True:
			transaction.die()
			self._end(transaction)
			return True
		else:
			return status

	def _find_site_apply_action(self, cmd, args, action):
		'''
		Find the site indicated by the singe number argument and apply the
		given action.
		'''

		check_args_len(cmd, args, 1)

		# Find the site. Will return early.
		index = int(args[0])
		for site in self._sites:
			if site.index is index:
				action(site)
				return True

		raise ValueError(cmd_error(cmd, args,
			'Site {} does not exist'.format(index)))

	def _fail(self, cmd, args):
		''' Fail site. '''

		def action(site):
			''' Apply site action. '''
			site.fail()
			self._log_at_time(None, 'site {} is down'.format(site.index))

		self._find_site_apply_action(cmd, args, action)

	def _recover(self, cmd, args):
		''' Recover site. '''

		def action(site):
			''' Apply site action. '''
			site.recover(self._tick)
			self._log_at_time(None, 'site {} is up'.format(site.index))

		self._find_site_apply_action(cmd, args, action)

	def _dump(self, cmd, args):
		''' Dump database state. '''

		if len(args) is 0:
			self._log_at_time(None, 'dumping all sites')
			print self.to_string(None)

		elif len(args) is 1:
			check_args_len(cmd, args, 1)

			if len(args[0]) is 0:
				raise ValueError(cmd_error(cmd, args,
					'Argument must match either [0-9]+ or x[0-9]+'))

			# See if this dump is for a site or a variable.
			is_site = False
			try:
				if args[0][0] == 'x':
					partition = int(args[0][1:])
					self._log_at_time(None,
							'dumping variable x{}'.format(partition))
				else:
					is_site = True
					partition = int(args[0])
					self._log_at_time(None,
							'dumping site S{}'.format(partition))
			except ValueError:
				raise ValueError(cmd_error(cmd, args,
					'Argument must match either [0-9]+ or x[0-9]+'))

			print self.to_string(partition, is_site)

	@staticmethod
	def _run_pending(transaction):
		''' Run pending commands and return number that were run. '''

		# Flush all commands that are possible to execute.
		num_run = 0
		while transaction.pending():
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
			'beginro': delegator('_beginro'),
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

		# Try to run all blocked transactions.
		for transaction in self._blocked_queue:
			if transaction.blocked() is not None:
				_, runner = transaction.blocked()
				if runner() is True:
					transaction.unblock()

		# Remove transactions no longer blocked.
		self._blocked_queue = [
				tx for tx in self._blocked_queue if tx.blocked() is not None]

		for cmd, args in commands:
			cmd_lower = cmd.lower()

			# Send commands to their delegates using function callbacks.
			if cmd_lower in self._COMMAND_DELEGATORS:
				self._COMMAND_DELEGATORS[cmd_lower](self, cmd, args)
			else:
				raise ValueError('Command {} is not recognized'
					.format(format_command(cmd, args)))

	def get_commit_abort_log(self):
		'''
		Get TransactionManager commit and abort log. Entries are of the form
			(TXID, TICK_END, STATUS)
		where status is one of TransactionManager.COMMITTED or
		TransactionManager.ABORTED.
		'''
		return tuple(self._commit_abort_log)

	# Field width used by __str__() method.
	_FIELD_WIDTH = 5

	def to_string(self, partition, is_site=False):
		'''
		Format as string. Partition can be None, a variable, or a site.

		The output is a matrix where each row is a site and each column is a
		variable. Each variable has an entry for whether or not it is available
		at call time. Partitions are row or column slices on the matrix.
		'''

		# Either we slice in rows or columns or None depending on the
		# partition.
		if partition is None or is_site is True:
			variable_fmt_str = '{}' + ('{}' * (len(self._variables) - 1))
			variables = self._variables
		else:
			variable_fmt_str = '{}'
			variables = (partition,)
		site_line_fmt_str = '{{:>3s}}:{}\n'.format(variable_fmt_str)

		if is_site is not True:
			sites = self._sites
		else:
			sites = (site for site in self._sites if site.index is partition)

		legend = [
				' x : denotes a variable',
				' S : denotes a site',
				' * : denotes that the variable is unavailable for reading',
				]
		legend_width = max(len(line) for line in legend)
		matrix_rule_len = max(
				legend_width,
				4 + (len(variables) * self._FIELD_WIDTH))

		matrix_rule = '-' * matrix_rule_len
		out = StringIO.StringIO()
		out.write(matrix_rule + '\n')

		out.write('    ')
		for variable in variables:
			# Shift left 1 space for the '*' available column.
			out.write('{{:>{}}} '.format(self._FIELD_WIDTH - 1)
					.format('x{}'.format(variable)))
		out.write('\n')
		out.write('    ')
		for variable in variables:
			out.write(('{{:>{}}}'.format(self._FIELD_WIDTH))
					.format('-' * (self._FIELD_WIDTH - 1)))
		out.write('\n')

		field_fmt = '{{:>{}}}'.format(self._FIELD_WIDTH)
		#field_fmt = '{{:{}d}}'.format(self._FIELD_WIDTH)
		not_present = field_fmt.format('- ')
		for site in sites:
			values, available = site.dump()
			variable_states = [
					field_fmt.format(
						str(values[variable]) +
						('*' if not available[variable] else ' '))
					if variable in values else not_present
					for variable in variables]
			out.write(site_line_fmt_str.format(
				'S{}'.format(site.index), *variable_states))

		out.write(matrix_rule + '\n')
		for line in legend:
			out.write(line + '\n')
		out.write(matrix_rule)
		out.seek(0)
		return out.read()

	def __str__(self):
		''' Represent the sites as a matrix. '''
		return self.to_string(None)


