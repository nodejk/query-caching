
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

import os

def define_box_properties(ax, plot_name, color_code, label):
    for k, v in plot_name.items():
        plt.setp(plot_name.get(k), color=color_code)

    # use plot function to draw a small line to name the legend.
    ax.plot([], c=color_code, label=label)
    ax.legend()

from result_parser import (
    get_child_name,
    get_sub_dir_names,
    get_sub_dir_paths,
    get_sub_nested_path,
    get_sub_dir_names_from_paths,
    create_graph,
    DerivativeDirParser,
)

def create_graph_general(
        args,
        var_key,
        y_variables,
        x_variables,
        graph_path,
        y_label,
        x_label,
    ):

    fig, ax = plt.subplots()

    print('variables-->', y_variables, x_variables, args)

    sorted_colors = sorted(
        mcolors.TABLEAU_COLORS
    )

    shift = 0

    fig_count, ax_count = plt.subplots()

    fig_cache, ax_cache = plt.subplots()

    fig_cache_count, ax_cache_count = plt.subplots()

    labels = []
    for i, derivative_par in enumerate(x_variables):

        label = derivative_par.get_meta_data()[var_key]

        fixed_vars = [key for key in args.keys() if key != var_key]

        print(label, var_key, fixed_vars)
        labels.append(label)

        derivative_data = []
        all_query_counts = []

        total_number_of_queries = []
        total_cache_hits = []

        for deriv in y_variables:
            _temp = args.copy()

            _temp[var_key] = deriv

            print(f'deriv: {deriv}')
            # flock -n /tmp/google_drv_sync.lock /usr/bin/rclone copy --transfers 20 --retries 5 "/home/blackplague/IdeaProjects/query-caching/experiments" "gdrive-uni:/query-caching-results"

            derivat_par = [item for item in derivative_par.get_experiment_results() if item.is_equal(_temp)][0]

            print([str(item) for item in derivative_par.get_experiment_results()])

            all_query_counts.append(derivat_par.get_execs_count())

            total_number_of_queries.append(derivat_par.get_total_number_of_queries())
            total_cache_hits.append(derivat_par.get_total_cache_hits())

            derivative_data.append([item.get_time_taken() for item in derivat_par.get_all_execs_info()])

        cache_hit_ratio = [total_cache_hits[i]/total_number_of_queries[i] for i in range(len(total_cache_hits))]
        cache_hit_count = [total_cache_hits[i] for i in range(len(total_cache_hits))]

        ax_count.plot(y_variables, all_query_counts, color=sorted_colors[i], label=label)
        ax_cache.plot(y_variables, cache_hit_ratio, color=sorted_colors[i], label=label)

        print(f"cache_hit_count: {cache_hit_count}")
        ax_cache_count.plot(y_variables, cache_hit_count, color=sorted_colors[i], label=label)

        # derivative_box_plot = ax.boxplot(
        #     derivative_data,
        #     positions=numpy.array(numpy.arange(len(derivative_data))) * 2 + shift,
        #     widths=0.5,
        #     meanline=True,
        #     showmeans=True,
        #     showfliers=False,
        # )

        ax.plot(derivative_data)

        shift += 0.35

        # define_box_properties(ax, derivative_box_plot, sorted_colors[i], label)

    ax.set_xticks(numpy.arange(0, len(y_variables) * 2, 2), y_variables)

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
    ax.set_ylabel(y_label)
    ax.set_xlabel(x_label)

    fig.savefig(f'{graph_path}/result_diff.png', bbox_inches='tight', dpi=120)

    pass
