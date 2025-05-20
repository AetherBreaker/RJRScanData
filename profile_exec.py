from cProfile import Profile
from pathlib import Path
from pstats import SortKey, Stats

OUTPUT = Path.cwd() / "profiler_output.txt"
sortby = SortKey.CUMULATIVE

if __name__ == "__main__":
  profiler = Profile(subcalls=False, builtins=False)
  profiler.enable()
  import exec

  profiler.disable()
  profiler.dump_stats(str(OUTPUT))
  stats = Stats(profiler).sort_stats(sortby).strip_dirs()
  stats.print_stats()
