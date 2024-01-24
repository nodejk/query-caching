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

import plotly.express as plt_express
import plotly.graph_objects as plt_graph_obj

def get_child_name(full_path: str):
    return pathlib.Path(full_path).parts[-1]


def get_sub_dir_names_from_paths(paths):
    return [get_child_name(path) for path in paths]


def get_sub_dir_names(path):
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

    def __init__(self, line, num_queries):
        payload_dict = json.loads(line.split('-')[-1])

        self._num_queries = num_queries
        self._time_taken = numpy.int64(payload_dict['time_taken'])

    def __str__(self):
        return f'_num_batches: {self._num_batches}, time_taken: {self._time_taken}'

    def get_num_query_batches(self):
        return self._num_batches

    def get_time_taken(self):
        return self._time_taken

    def get_num_queries(self):
        return self._num_queries

    def get_average_query_time(self):
        return self._time_taken / self._num_queries

class CacheHit:
    __cache_hit: int
    temp: int

    def __init__(self, line):

        found = re.search(CacheHit.regex_string(), line)
        payload_dict = json.loads(line)

        self.__cache_hit = numpy.int64(payload_dict['cache_hits'])
        self.temp = numpy.int64(payload_dict['total_queries'])

        print(f'total_queries: {self.temp}')
    
    def get_cache_hit(self):
        return self.__cache_hit

    def get_num_queries(self):
        return self.temp

    @staticmethod
    def regex_string():
        return r"{\"cache_hits\": \"\d\", \"total_queries\": \"\d\"}"

    @staticmethod
    def check_if_regex_exists(line):
        _temp = re.search(CacheHit.regex_string(), line)

        return _temp is not None
    

class ResultParser:

    def __init__(self):
        pass


class ResultFileParser:
    _cache: int
    _mode_type: str
    _cache_size: str
    _query_type: str
    _derivative: int
    _experiment: str
    _cache_size: int
    _query_type: str

    _execs_info: typing.List[Exec]
    _all_cache_hits: typing.List[CacheHit]

    _execs_info: typing.List[Exec]
    _cache_info: typing.List[CacheHit]

    def __str__(self):
        return ', '.join([f'_cache: {self._experiment}',
                f'_mode: {self._mode_type}',
                f'_cache_size: {self._cache_size}',
                f'_query_type: {self._query_type}',
                f'_derivative: {self._derivative}'])
    
    def get_average_time_required(self):
        return self.get_total_time_required() / self.get_total_number_of_queries_executed()

    def get_total_cache_hits(self):
        return sum([i.get_cache_hit() for i in self._all_cache_hits])

    def get_total_time_required(self):
        return sum([i.get_time_taken() for i in self._execs_info])

    def get_cache_hits(self):
        return numpy.array([item.get_num_cache_hits() for item in self._cache_info])

    def get_number_of_queries(self):
        return sum([item.get_total_num_queries() for item in self._cache_info])

    def get_total_query_time(self):
        return sum([item.get_time_taken() for item in self._execs_info])
    
    def get_total_number_of_queries(self):
        return sum([item.get_num_queries() for item in self._all_cache_hits])

    # def get_total_cache_hits(self):
    #     return sum([item.get_num_cache_hits() for item in self._cache_info])

    def get_execs_count(self):
        return len(self._execs_info)

    def get_all_execs_info(self):
        return self._execs_info

    def get_derivative(self):
        return self._derivative

    def get_experiment(self):
        return self._experiment

    def get_total_number_of_queries_executed(self):
        return sum([item.get_num_queries() for item in self._all_cache_hits])

    def get_meta_data(self):
        return {
            'experiment': self._experiment,
            'mode': self._mode_type,
            'cache_size': self._cache_size,
            'query_type': self._query_type,
            'derivate': self._derivative,
        }

    def __init__(
        self, 
        file_path, 
        derivative, 
        experiment,
        mode_type,
        cache_size,
        query_type,
    ):

        self._experiment = experiment
        self._mode_type = mode_type
        self._cache_size = cache_size
        self._query_type = query_type

        self._execs_info = []
        self._all_cache_hits = []
        self._derivative = derivative

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

                    num_queries = 0

                    test = [i for i in required_lines if 'No. of queries:' in i]

                    num_queries = int(test[0].replace('No. of queries: ', ''))

                    _temp = [i for i in required_lines if 'cache_hits' in i]
                    _line = _temp[0]

                    print(f'_line: {_line}')

                    self._execs_info.append(Exec(required_lines[-1], num_queries))

                    # for __line in required_lines: 
                    #     if CacheHit.check_if_regex_exists(__line):
                    self._all_cache_hits.append(CacheHit(_line))
                else:
                    index += 1


        # print('self._execs_info---->', len(self._execs_info), ' derivative-->', derivative)
        
        # print('self._execs_info---->', len(self._execs_info), ' derivative-->', derivative)
        # pass


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
        query_type,
    ):
        self._experiment_results = []

        self.experiment = experiment
        self.mode_type = mode_type
        self.cache_size = cache_size
        self.query_type = query_type

        for path in directory_paths:
            derivative = get_child_name(path)
            
            self._experiment_results.append(
                ResultFileParser(
                    file_path=convert_derivative_path_to_result(path),
                    derivative=derivative, 
                    experiment=experiment,
                    mode_type=mode_type,
                    cache_size=cache_size,
                    query_type=query_type,            
                )
            )

            # self._experiment_results.append(
            #     ResultFileParser(
            #         file_path=convert_derivative_path_to_result(path),
            #         _cache=_cache,
            #         _mode=_mode,
            #         _cache_size=_cache_size,
            #         _query_type=_query_type,
            #         _derivative=_derivative,
            #         _dimension_type=_dimension_type,
            #     )
            # )

    def get_experiment_results(self):
        return self._experiment_results

def define_box_properties(ax, plot_name, color_code, label):
    for k, v in plot_name.items():
        plt.setp(plot_name.get(k), color=color_code)

    # use plot function to draw a small line to name the legend.
    ax.plot([], c=color_code, label=label)
    ax.legend()


def create_dataframe_plot(data):

    CACHING_POLICY = 'Caching Policy'
    CACHE_SIZE_IN_MB = 'Cache Size (in MB)'
    CHANGE_IN_PERCENT = 'Change (in percentage)'

    results_dataframe = pandas.DataFrame(numpy.array(data), columns=[
        CACHING_POLICY, CACHE_SIZE_IN_MB, 'val'
    ])

    results_dataframe['val'] = results_dataframe['val'].astype(float)

    results_dataframe_agg = results_dataframe.groupby([CACHING_POLICY]).agg({
        'val': numpy.min
    })

    results_dataframe_agg.rename(columns={
        'val': 'val_max',
    }, inplace=True)

    results_dataframe_agg.reset_index(inplace=True)

    # results_dataframe.sort_values(by=[
    #     CACHING_POLICY, CACHE_SIZE_IN_MB
    # ])


    results_dataframe = results_dataframe.merge(
        results_dataframe_agg, how='left', on=[CACHING_POLICY]
    )

    results_dataframe[CHANGE_IN_PERCENT] = 100.0 * (results_dataframe['val_max'] - results_dataframe['val'])/results_dataframe['val_max']

    
    results_dataframe.drop(
        ['val', 'val_max'], axis=1, inplace=True
    )

    results_dataframe[CHANGE_IN_PERCENT] = -1 * results_dataframe[CHANGE_IN_PERCENT].round(2)

    results_dataframe = pandas.pivot_table(
        results_dataframe, 
        values=CHANGE_IN_PERCENT, 
        index=CACHING_POLICY, 
        columns=[CACHE_SIZE_IN_MB], sort=False).fillna(0)

    fig = plt_express.imshow(results_dataframe, x=results_dataframe.columns, y=results_dataframe.index, text_auto=True)
    fig = plt_graph_obj.Figure(data=fig.data, layout=fig.layout)
    fig = fig.update_traces(text=results_dataframe.applymap(lambda x: x).values, texttemplate="%{text}%", hovertemplate=None, xgap=5, ygap=5)

    # fig.show()


def create_graph(
        args, 
        var_key, 
        variables, 
        x_vars, 
        y_vars: typing.List[ResultFileParser], 
        graph_path,
        experiments,
    ):

    sorted_colors = sorted(
        mcolors.TABLEAU_COLORS
    )

    shift = 0

    fig_time, ax_time = plt.subplots()
    fig_count, ax_count = plt.subplots()

    all_results = []

    for i, experiment in enumerate(y_vars):

        derivative_data = []
        all_query_time = []
        all_query_count = []

        experiment_label = None


        for result in experiment:
            experiment_label = result.get_experiment()
            _temp_derivative = result.get_derivative()
            
            label = result.get_meta_data()[var_key]


            print(f'result: {result} {result.get_total_query_time()} {result.get_total_number_of_queries_executed()}')

            all_query_time.append(result.get_total_query_time())
            all_query_count.append(result.get_total_cache_hits() / 320)
            derivative_data.append(label)

            
            # if experiment_label == 'mru' and _temp_derivative == '10':
            #     sys.exit()
            #     all_results.append([result.get_experiment(), label, 1420])
            # else:

            all_results.append([result.get_experiment(), label, result.get_average_time_required()])

        derivative_box_plot = ax_time.plot(
            derivative_data,
            all_query_time,
            label=experiment_label,
            # positions=numpy.array(numpy.arange(len(derivative_data))) * 2 + shift,
            # widths=0.5,
            # meanline=True,
            # showmeans=True,
            # showfliers=False,
        )

        ax_count.plot(
            derivative_data,
            all_query_count,
            label=experiment_label
        )

    # create_dataframe_plot(all_results)
    print(all_results)
    ax_count.legend()
    ax_time.legend()

    ax_time.set_ylabel('Time in (s)')
    ax_time.set_xlabel('Cache size (in MB)')

    ax_count.set_ylabel('Cache hit (in %)')
    ax_count.set_xlabel('Cache size (in MB)')

    f"cache type: {args['experiment']}"
    f"mode_type: {args['mode']}"
    f"cache_size: {args['cache_size']}"
    f"query_type: {args['query_type']}"

    print(f'path: {graph_path}/result.png')
    fig_time.savefig(f'{graph_path}/result_time.png', dpi=120)
    fig_count.savefig(f'{graph_path}/result_count.png', dpi=120)

    pass


if __name__ == '__main__':
    example_file = '/Users/new_horizon/query-caching/query-caching-results/rr/hybrid/4/all/10/size_bytes/result.txt'

    result = ResultFileParser(
        file_path=example_file, 
        derivative='10', 
        experiment='rr',
        mode_type='hybrid',
        cache_size='4',
        query_type='all',
    )

    print(result.get_average_time_required())
    print(result.get_total_time_required())
    print(f'{len(result.get_all_execs_info())}')
