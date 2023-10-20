import json
import numpy
import typing
import os
import re
import random
import pandas
import matplotlib.colors as mcolors
from matplotlib.offsetbox import AnchoredText
import matplotlib.pyplot as plt
import numpy
import glob
import pathlib

import seaborn


def get_child_name(full_path: str):
    return pathlib.Path(full_path).parts[-1]


def get_sub_dir_names_from_paths(paths):
    return [get_child_name(path) for path in paths]


def get_sub_dir_names(path):
    print(path)
    return [get_child_name(f) for f in os.listdir(f'{path}/')]


def get_sub_dir_paths(path, items):
    return [os.path.join(path, item) for item in items]


def get_sub_nested_path(paths):
    return [os.path.join(path, get_child_name(item)) for path in paths for item in os.listdir(f'{path}/')]


def convert_derivative_path_to_result(path):
    return os.path.join(path, 'size_bytes', 'result.txt')


class Exec:
    _num_batches: int
    _time_taken: numpy.int64

    def __init__(self, line):
        payload_dict = json.loads(line.split('-')[-1])

        self._num_batches = int(payload_dict['num_queries'])
        self._time_taken = numpy.int64(payload_dict['time_taken'])

    def __str__(self):
        return f'_num_batches: {self._num_batches}, time_taken: {self._time_taken}'

    def get_num_query_batches(self):
        return self._num_batches

    def get_time_taken(self):
        return self._time_taken

class CacheInfo:
    _total_num_queries: int
    _cache_hits: int

    def __init__(self, line):

        found = re.search(CacheInfo.regex_string(), line)

        payload_dict = json.loads(found.group())

        self._cache_hits = float(payload_dict['cache_hits'])
        self._total_num_queries = float(payload_dict['total_queries'])

    def __str__(self):
        return f'cache_hits: {self._cache_hits}, total_num_queries: {self._total_num_queries}'

    def get_num_cache_hits(self):
        return self._cache_hits

    def get_total_num_queries(self):
        return self._total_num_queries

    @staticmethod
    def regex_string():
        return r"{\"cache_hits\": \"\d\", \"total_queries\": \"\d\"}"

    @staticmethod
    def check_if_regex_exists(line):
        return re.search(CacheInfo.regex_string(), line) is not None


class ResultParser:

    def __init__(self):
        pass


class ResultFileParser:
    _cache: int
    _mode: str
    _cache_size: str
    _query_type: str
    _derivative: int
    _dimension_type: str

    _execs_info: typing.List[Exec]
    _cache_info: typing.List[CacheInfo]

    def __str__(self):
        return (f'_cache: {self._cache}'
                f'_mode: {self._mode}'
                f'_cache_size: {self._cache_size}'
                f'_query_type: {self._query_type}'
                f'_derivative: {self._derivative}'
                f'_dimension_type: {self._dimension_type}')
    def get_average_time_required(self):
        return numpy.mean(
            numpy.array([item.get_num_query_batches() for item in self._execs_info])
        )

    def get_cache_hits(self):
        return numpy.array([item.get_num_cache_hits() for item in self._cache_info])

    def get_number_of_queries(self):
        return numpy.array([item.get_total_num_queries() for item in self._cache_info])

    def get_total_number_of_queries(self):
        return sum([item.get_total_num_queries() for item in self._cache_info])

    def get_total_cache_hits(self):
        return sum([item.get_num_cache_hits() for item in self._cache_info])

    def get_execs_count(self):
        return len(self._execs_info)

    def get_all_execs_info(self):
        return self._execs_info

    def get_derivative(self):
        return self._derivative

    def get_total_number_of_queries_batches_executed(self):
        return numpy.sum(
            numpy.array([item.get_num_query_batches() for item in self._execs_info])
        )

    def is_equal(self, other):
        print(f'other: {other}')
        print(f"self._cache: {self._cache == other['cache']}")
        print(f"self._mode: {self._mode == other['mode']}")
        print(f"self._cache_size: {self._cache_size == other['cache_size']} {self._cache_size} {other['cache_size']}")
        print(f"self._query_type: {self._query_type == other['query_type']}")
        print(f"self._derivative: {self._derivative == other['derivative']}")

        if (
            self._cache == other["cache"] and
            self._mode == other["mode"] and
            self._cache_size == other["cache_size"] and
            self._query_type == other["query_type"] and
            self._derivative == other["derivative"]
        ): return True

        return False

    def __init__(
        self,
        file_path,
        _cache: int,
        _mode: str,
        _cache_size: str,
        _query_type: str,
        _derivative: str,
        _dimension_type: str,
    ):

        self._execs_info = []
        self._cache_info = []

        self._derivative = _derivative
        self._cache = _cache
        self._mode = _mode
        self._cache_size = _cache_size
        self._query_type = _query_type
        self._dimension_type = _dimension_type

        print('file_path-->', file_path)

        with open(file_path, 'r') as f:
            lines = f.readlines()

            index = 0

            while index < len(lines):
                line = lines[index]

                clean_line = line.strip()

                if clean_line == 'START==============================================':
                    start_index = index

                    line_stop = lines[index]
                    line_stop = line_stop.strip()

                    while line_stop[:6] != '[EXEC]':
                        line_stop = lines[index]
                        line_stop = line_stop.strip()

                        index += 1

                        if index >= len(lines):
                            break

                    if index >= len(lines):
                        break

                    required_lines = [line.strip() for line in lines[start_index:index]]

                    self._execs_info.append(Exec(required_lines[-1]))

                    for _line in required_lines:
                        if CacheInfo.check_if_regex_exists(_line):
                            self._cache_info.append(CacheInfo(_line))
                else:
                    index += 1


class DerivativeDirParser:
    experiment: str
    mode: str
    cache_size: int
    query_type: str

    _experiment_results: typing.List[ResultFileParser]

    def get_meta_data(self):
        return {
            'experiment': self.experiment,
            'mode': self.mode_type,
            'cache_size': self.cache_size,
            'query_type': self.query_type,
        }

    def is_equal(self, other):
        print(f'other: {other}')
        print(f"self._cache: {self._cache == other['cache']}")
        print(f"self._mode: {self._mode == other['mode']}")
        print(f"self._cache_size: {self._cache_size == other['cache_size']} {self._cache_size} {other['cache_size']}")
        print(f"self._query_type: {self._query_type == other['query_type']}")
        print(f"self._derivative: {self._derivative == other['derivative']}")

        if (
                self._cache == other["cache"] and
                self._mode == other["mode"] and
                self._cache_size == other["cache_size"] and
                self._query_type == other["query_type"] and
                self._derivative == other["derivative"]
        ): return True

        return False

    def __init__(self,
        directory_paths,
        experiment,
        mode_type,
        cache_size,
        query_tye,
    ):
        self._experiment_results = []

        self.experiment = experiment
        self.mode_type = mode_type
        self.cache_size = cache_size
        self.query_type = query_tye

        for path in directory_paths:
            _cache = path.split('/')[-5:][0]
            _mode = path.split('/')[-5:][1]
            _cache_size = path.split('/')[-5:][2]
            _query_type = path.split('/')[-5:][3]
            _derivative = path.split('/')[-5:][4]
            _dimension_type = 'size_bytes'


            print(
                f'_cache: {_cache},'
                f'_mode: {_mode},'
                f'_cache_size: {_cache_size},'
                f'_query_type: {_query_type},'
                f'_derivative: {_derivative},'
                f'_dimension_type: {_dimension_type}'
            )

            self._experiment_results.append(
                ResultFileParser(
                    file_path=convert_derivative_path_to_result(path),
                    _cache=_cache,
                    _mode=_mode,
                    _cache_size=_cache_size,
                    _query_type=_query_type,
                    _derivative=_derivative,
                    _dimension_type=_dimension_type,
                )
            )

    def get_experiment_results(self):
        return self._experiment_results

def define_box_properties(ax, plot_name, color_code, label):
    for k, v in plot_name.items():
        plt.setp(plot_name.get(k), color=color_code)

    # use plot function to draw a small line to name the legend.
    ax.plot([], c=color_code, label=label)
    ax.legend()

def create_graph(args, var_key, variables, derivatives, derivatives_parsed: typing.List[DerivativeDirParser], graph_path):

    fig, ax = plt.subplots()

    print('variables-->', variables, derivatives, len(derivatives_parsed))

    sorted_colors = sorted(
        mcolors.TABLEAU_COLORS
    )

    shift = 0

    fig_count, ax_count = plt.subplots()

    fig_cache, ax_cache = plt.subplots()

    fig_cache_count, ax_cache_count = plt.subplots()

    for i, derivative_par in enumerate(derivatives_parsed):

        label = derivative_par.get_meta_data()[var_key]

        derivative_data = []
        all_query_counts = []

        total_number_of_queries = []
        total_cache_hits = []

        for deriv in derivatives:
            derivat_par = [item for item in derivative_par.get_experiment_results() if item.get_derivative() == deriv][0]

            all_query_counts.append(derivat_par.get_execs_count())

            total_number_of_queries.append(derivat_par.get_total_number_of_queries())
            total_cache_hits.append(derivat_par.get_total_cache_hits())

            derivative_data.append([item.get_time_taken() for item in derivat_par.get_all_execs_info()])

        cache_hit_ratio = [total_cache_hits[i]/total_number_of_queries[i] for i in range(len(total_cache_hits))]
        cache_hit_count = [total_cache_hits[i] for i in range(len(total_cache_hits))]

        ax_count.plot(derivatives, all_query_counts, color=sorted_colors[i], label=label)
        ax_cache.plot(derivatives, cache_hit_ratio, color=sorted_colors[i], label=label)

        print(f"cache_hit_count: {cache_hit_count}")
        ax_cache_count.plot(derivatives, cache_hit_count, color=sorted_colors[i], label=label)

        derivative_box_plot = ax.boxplot(
            derivative_data,
            positions=numpy.array(numpy.arange(len(derivative_data))) * 2 + shift,
            widths=0.5,
            meanline=True,
            showmeans=True,
            showfliers=False,
        )

        shift += 0.35

        define_box_properties(ax, derivative_box_plot, sorted_colors[i], label)

    ax.set_xticks(numpy.arange(0, len(derivatives) * 2, 2), derivatives)

    # fig.subplots_adjust(right=1.5) # or whatever    ax.legend()
    ax_count.legend()

    ax_cache.set_title(
        f"cache type: {args['experiment']} "
        f"mode_type: {args['mode']} "
        f"cache_size: {args['cache_size']} "
        f"query_type: {args['query_type']}"
    )

    ax.set_title(
        f"cache type: {args['experiment']} "
        f"mode_type: {args['mode']} "
        f"cache_size: {args['cache_size']} "
        f"query_type: {args['query_type']}"
    )
    ax.set_ylabel('time in ms')
    ax.set_xlabel('derivability percentage')

    ax_count.set_ylabel('Number of executed batches of queries executed')
    ax_count.set_xlabel('Derivability percentage')

    ax_cache.set_ylabel('Ratio of cache hit to total number of queries')
    ax_cache.set_xlabel('Derivability percentage')


    ax_cache_count.set_ylabel('Number of cache hits')
    ax_cache_count.set_xlabel('Derivability percentage')

    ax_cache_count.legend()

    ax_cache.legend()

    f"cache type: {args['experiment']}"
    f"mode_type: {args['mode']}"
    f"cache_size: {args['cache_size']}"
    f"query_type: {args['query_type']}"

    fig.savefig(f'{graph_path}/result.png', bbox_inches='tight', dpi=120)

    print(f'saving to... {graph_path}')

    fig_count.savefig(f'{graph_path}/result_count.png', bbox_inches='tight', dpi=120)
    fig_cache.savefig(f'{graph_path}/result_cache_hit_ratio.png', bbox_inches='tight', dpi=120)
    fig_cache_count.savefig(f'{graph_path}/result_cache_count.png', bbox_inches='tight', dpi=120)

    pass


if __name__ == '__main__':
    example_file = '/home/blackplague/IdeaProjects/query-caching/experiments/fifo/sequence/4/all/10/size_bytes/result.txt'

    result = ResultFileParser(example_file)

    print([str(i) for i in result._execs_info])
