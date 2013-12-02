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
from repcrec.commands import CommandStreamReader, TestFile
