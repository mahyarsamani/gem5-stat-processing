import re

from .stats import Stats, ScalarStat
from .string_manip import remove_numerical_characters


def process_stats_file(stats_file) -> Stats:
    stats = Stats()
    for line in stats_file.readlines():
        if re.match(r"\S+\.\w+\s[^\|]+", line):
            tokens = line.split()
            owner, name = tokens[0].rsplit(".", 1)
            value = float(tokens[1])
            desc = " ".join(tokens[3:])
            owner_group = remove_numerical_characters(owner)
            stat_entry = stats.find(owner_group, name)
            if stat_entry is None:
                stat_entry = stats.insert(
                    owner_group, name, ScalarStat(name, desc)
                )
            stat_entry.add_to_container((owner, value))
    return stats
