from __future__ import annotations
from slurm_monitor.db.v2.db_tables import ErrorMessage
import dataclasses
from enum import Enum
import re

@dataclasses.dataclass
class Meta:
    producer: str
    version: str

@dataclasses.dataclass
class Data:
    type: str
    attributes: dict[str, any]

@dataclasses.dataclass
class Message:
    meta: Meta
    data: Data | None
    errors: list[ErrorMessage] | None


class TopicType(str, Enum):
    cluster = 'cluster'
    job = 'job'
    sample = 'sample'
    sysinfo = 'sysinfo'

    def get_topic(self, cluster: str) -> str:
        return f"{cluster}.{self.value}"

    @classmethod
    def infer(cls, topic: str) -> TopicType:
        for t in cls:
            if topic.endswith(f".{t.value}"):
                return t
        raise ValueError("TopicType.infer: '{topic}' does not have a value topic type suffix")

class Sonar:
    @classmethod
    def expand_simple_range(cls, hostname_range: str) -> list[str]:
        cls.validate_simple_range(hostname_range)

        expanded = []
        sections = hostname_range[1:-1].split(",")
        for section in sections:
            range_expr = section.split("-")
            if len(range_expr) == 1:
                expanded.append(range_expr[0])
            elif len(range_expr) == 2:
                start, end = range_expr
                pattern_length = len(start)
                if not pattern_length == len(end):
                    raise ValueError("Pattern length of range does not match: "
                                     f"from {pattern_length} - {len(end)}"
                    )

                use_zfill = False
                if start.startswith("0"):
                    use_zfill = True

                for i in range(int(start), int(end)+1):
                    number = i
                    if use_zfill:
                        number = str(i).zfill(pattern_length)

                    expanded.append(number)
            else:
                raise ValueError(f"Invalid pattern encountered for range: {range_expr}")

        return expanded

    @classmethod
    def validate_simple_range(cls, hostname_range: str):
        m = re.match(r"\[[\d,-]+\]", hostname_range)
        if not m:
            raise ValueError(f"Given expression: '{hostname_range}' is not a hostname range")


    @classmethod
    def expand_hostname_range(cls, hostname_range: list | str) -> list[str]:
        """
        Expand hostname range so that processing `c[1-3,5]-[2-4].fox` yields:
            `c1-2.fox`, `c1-3.fox`, `c1-4.fox`, `c2-2.fox`, `c2-3.fox`, `c2-4.fox`, `c3-2.fox`, `c3-3.fox`, `c3-4.fox`, `c5-2.fox`, ...
        """

        nodes = []
        if type(hostname_range) is list:
            for h in hostname_range:
                nodes += cls.expand_hostname_range(h)
            return nodes

        if type(hostname_range) is not str:
            raise ValueError(f"Expansion requires a string representation, not {type(hostname_range)} ({hostname_range})")

        # split at comma that are not within brackets
        ranges = re.split(r',\s*(?![^\[]*\])', hostname_range)

        for hostnames_expr in ranges:
            includes_ranges = re.findall(r'(?P<pre>[^ \[\]]*)(?P<range>\[[\d,-]+\])(?P<post>[^ \[\]]*)', hostnames_expr)
            if not includes_ranges:
                # single node name
                nodes.append(hostnames_expr.strip())

            # Now we need to iterate over all ranges
            # a single match is now a triple (pre, range, post), where pre and post an constant elements
            # we will create a set of partical completions that will be continously extended base on the encountered ranges
            nodenames = []
            for match_group in includes_ranges:
                pre, hostname_range, post = match_group
                if nodenames:
                    nodenames = [x+pre for x in nodenames]
                else:
                    nodenames = [pre]
                expanded = cls.expand_simple_range(hostname_range)
                nodenames = [n+str(e) for n in nodenames for e in expanded]
                nodenames = [x+post for x in nodenames]

            nodes += nodenames

        return nodes
