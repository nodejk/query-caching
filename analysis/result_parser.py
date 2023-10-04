import json
import numpy
import typing
import os
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
    _num_queries: int
    _time_taken: numpy.int64

    def __init__(self, line):
        payload_dict = json.loads(line.split('-')[-1])

        self._num_queries = int(payload_dict['num_queries'])
        self._time_taken = numpy.int64(payload_dict['time_taken'])

    def __str__(self):
        return f'num_queries: {self._num_queries}, time_taken: {self._time_taken}'

    def get_num_queries(self):
        return self._num_queries

    def get_time_taken(self):
        return self._time_taken

class ResultParser:

    def __init__(self):
        pass


class ResultFileParser:
    _derivative: int
    _execs_info: typing.List[Exec]

    def get_average_time_required(self):
        return numpy.mean(
            numpy.array([item.get_num_queries() for item in self._execs_info])
        )

    def get_execs_count(self):
        return len(self._execs_info)

    def get_all_execs_info(self):
        return self._execs_info

    def get_derivative(self):
        return self._derivative

    def get_total_number_of_queries_executed(self):
        return numpy.sum(
            numpy.array([item.get_num_queries() for item in self._execs_info])
        )

    def __init__(self, file_path, derivative):

        self._execs_info = []
        self._derivative = derivative


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

                else:
                    index += 1

        print('self._execs_info---->', len(self._execs_info), ' derivative-->', derivative)
        pass


class DerivativeDirParser:
    experiment: str
    mode: str
    cache_size: int
    query_type: str

    experiment_results: typing.List[ResultFileParser]

    def get_meta_data(self):
        return {
            'experiment': self.experiment,
            'mode': self.mode_type,
            'cache_size': self.cache_size,
            'query_type': self.query_type,
        }

    def __init__(self,
        directory_paths,
        experiment,
        mode_type,
        cache_size,
        query_tye,
    ):
        self.experiment_results = []

        self.experiment = experiment
        self.mode_type = mode_type
        self.cache_size = cache_size
        self.query_type = query_tye

        for path in directory_paths:
            derivative = get_child_name(path)
            self.experiment_results.append(
                ResultFileParser(convert_derivative_path_to_result(path), derivative)
            )


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


    for i, derivative_par in enumerate(derivatives_parsed):

        label = derivative_par.get_meta_data()[var_key]

        derivative_data = []
        all_query_counts = []

        for deriv in derivatives:
            derivat_par = [item for item in derivative_par.experiment_results if item.get_derivative() == deriv][0]

            all_query_counts.append(derivat_par.get_execs_count())

            derivative_data.append([item.get_time_taken() for item in derivat_par.get_all_execs_info()])

        ax_count.plot(derivatives, all_query_counts, color=sorted_colors[i], label=label)

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

    ax.legend()
    ax_count.legend()

    ax.set_title(
        f"cache type: {args['experiment']} "
        f"mode_type: {args['mode']} "
        f"cache_size: {args['cache_size']} "
        f"query_type: {args['query_type']}"
    )
    ax.set_ylabel('time in ms')
    ax.set_xlabel('derivibility percentage')

    f"cache type: {args['experiment']}"
    f"mode_type: {args['mode']}"
    f"cache_size: {args['cache_size']}"
    f"query_type: {args['query_type']}"

    fig.savefig(f'{graph_path}/result.png', dpi=120)


    fig_count.savefig(f'{graph_path}/result_count.png', dpi=120)

    pass


if __name__ == '__main__':
    example_file = '/home/blackplague/IdeaProjects/query-caching/experiments/fifo/sequence/4/all/10/size_bytes/result.txt'

    result = ResultFileParser(example_file)

    print([str(i) for i in result._execs_info])
