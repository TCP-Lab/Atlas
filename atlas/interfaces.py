"""Interface module, containing all interfaces for Atlas.

###################   >>>  INTERFACE BEST PRACTICES  <<<    ####################
1. Try and reuse old downloaders and processors, if possible.
2. Keep in mind that errors do not get raised immediately. Instead, the
   interface catches them and returns them to the main process. It is after
   all processes complete than Atlas checks to see if any processes have raised
   an error and then re-raises it. This is similar to what `ProcessPoolExecutor`
   already does, but the stack trace is smaller, and we can get what interface
   died with what error, and log it.
3. Keep in mind that the pool is not CTRL+C friendly. The pool catches the
   interrupt, stops making new children, waits until they are all finished
   or dead, and *then* stops, re-raising the KeyboardInterrupt.
   If you really want, use CTRL-C twice to kill the handler. But this might leave
   orphaned processes.
4. Interfaces should all return a pd.DataFrame with the downloaded and
   (optionally) processed data. These dataframes should all contain a single
   "pivot" column that depends on the type of the interface. Atlas will
   automatically detect this common column, and perform a per-row merge
   of the dataframes with this single pivot column. The dataframe is then
   saved to disk.
5. It is YOUR RESPONSIBILITY to ensure the following:
    - The interface names MUST be unique. This is because the queries rely on
      the interface names to be fulfilled.
    - Interfaces with the same type MUST be return ONE, and ONLY ONE, column
      in common. Atlas will trigger a warning (not an error) if there are
      multiple columns in common.
    - Interfaces MUST fulfill their `.provided_cols` promise. Atlas will trigger
      a warning (not an error) if this is not the case.
6. Interfaces not added to the ALL_INTERFACES variable will not be loaded by
   Atlas. This is a good way to remove test interfaces from Atlas while not
   developing the tool.
"""
# TODO: Add some way to test point n. 5 above in automated deployment tests.
from functools import partial

from tqdm import tqdm

from atlas import OPTIONS
from atlas.test_interfaces import ALL_TEST_INTERFACES

down_tqdm = partial(tqdm, leave=False, colour="BLUE")
process_tqdm = partial(tqdm, leave=False, colour="GREEN")


###############################################################################
# Specify which interfaces are part of Atlas.
ALL_INTERFACES = []

# Add test interfaces if we are in debug mode
if OPTIONS["debugging"]["include_test_interfaces"]:
    ALL_INTERFACES.extend(ALL_TEST_INTERFACES)
###############################################################################
