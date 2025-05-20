import os
from pathlib import Path
from pstats import SortKey, Stats, add_func_stats, func_get_function_name, func_std_string

OUTPUT = Path.cwd() / "profiler_output.txt"
sortby = SortKey.CUMULATIVE


PROJECT_NAME = "RJRScanData"


def func_strip_path(func_name):
  filename, line, name = func_name
  return os.path.basename(filename), line, name


def fixed_func_strip_path(func_name):
  filename, line, name = func_name
  pathpath = Path(filename)

  if "site-packages" in pathpath.parts:
    libname_index = pathpath.parts.index("site-packages") + 1
  elif PROJECT_NAME in pathpath.parts:
    libname_index = pathpath.parts.index(PROJECT_NAME)
  elif "Lib" in pathpath.parts:
    libname_index = pathpath.parts.index("Lib") + 1
  else:
    libname_index = 0

  libpath = ".".join(pathpath.parts[libname_index:])

  return libpath, line, name


class FixedStats(Stats):
  def strip_dirs(self):
    oldstats = self.stats
    self.stats = newstats = {}
    max_name_len = 0
    for func, (cc, nc, tt, ct, callers) in oldstats.items():
      newfunc = fixed_func_strip_path(func)
      if len(func_std_string(newfunc)) > max_name_len:
        max_name_len = len(func_std_string(newfunc))
      newcallers = {}
      for func2, caller in callers.items():
        newcallers[fixed_func_strip_path(func2)] = caller

      if newfunc in newstats:
        newstats[newfunc] = add_func_stats(newstats[newfunc], (cc, nc, tt, ct, newcallers))
      else:
        newstats[newfunc] = (cc, nc, tt, ct, newcallers)
    old_top = self.top_level
    self.top_level = new_top = set()
    for func in old_top:
      new_top.add(fixed_func_strip_path(func))

    self.max_name_len = max_name_len

    self.fcn_list = None
    self.all_callees = None
    return self

  def print_stats(self, *amount):
    for filename in self.files:
      print(filename, file=self.stream)
    if self.files:
      print(file=self.stream)
    indent = " " * 8
    for func in self.top_level:
      print(indent, func_get_function_name(func), file=self.stream)

    print(indent, self.total_calls, "function calls", end=" ", file=self.stream)
    if self.total_calls != self.prim_calls:
      print("(%d primitive calls)" % self.prim_calls, end=" ", file=self.stream)
    print("in %.3f seconds" % self.total_tt, file=self.stream)
    print(file=self.stream)
    width, list = self.get_print_list(amount)
    if list:
      self.print_title()
      for func in list:
        if func[0].startswith(PROJECT_NAME):
          self.print_line(func)
      print(file=self.stream)
      print(file=self.stream)
    return self


if __name__ == "__main__":
  if OUTPUT.exists():
    stats = FixedStats(str(OUTPUT))
    stats.strip_dirs()
    stats.sort_stats(sortby)
    stats.print_stats()
