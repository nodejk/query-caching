import os

from result_parser import (
    get_child_name,
    get_sub_dir_names,
    get_sub_dir_paths,
    get_sub_nested_path,
    get_sub_dir_names_from_paths,
    create_graph,
    DerivativeDirParser,
)


def build_experiment_graph(root_path, experiment, mode, cache_size, query_type, derivatives):
    path = f'{root_path}/{experiment}'
    modes2 = [i for i in get_sub_dir_names(path) if i == mode]

    mode_paths = get_sub_dir_paths(path, modes2)

    cache_items = get_sub_nested_path(mode_paths)

    for mode_path in mode_paths:

        cache_sizes = [i for i in get_sub_dir_names(mode_path) if i == cache_size]
        cache_paths = get_sub_dir_paths(mode_path, cache_sizes)

        for cache_path in cache_paths:
            query_types = [i for i in get_sub_dir_names(cache_path) if i == query_type]
            query_paths = get_sub_dir_paths(cache_path, query_types)

            for query_path in query_paths:
                dervibility_types = [i for i in get_sub_dir_names(query_path) if i in derivatives]
                dervibility_paths = get_sub_dir_paths(query_path, dervibility_types)

                return DerivativeDirParser(
                    directory_paths=dervibility_paths,
                    experiment=experiment,
                    mode_type=get_child_name(mode_path),
                    cache_size=get_child_name(cache_path),
                    query_tye=get_child_name(query_path),
                )


def get_derivatives(args, root_path):
    all_constants = []
    all_variables = []
    var_key = None

    for key in args.keys():
        val = args[key]

        if isinstance(val, list):
            var_key = key
            all_variables.append(val)
        else:
            all_constants.append(val)

    all_derivatives = []

    args['derivatives'] = ['10', '25', '45', '75', '90']


    if len(all_variables) != 1:
        raise Exception('only one var allowed')

    for var in all_variables[0]:
        args[var_key] = var
        print(args)
        all_derivatives.append(build_experiment_graph(root_path, **args))

    return all_derivatives, {'var_key': var_key, 'variables': all_variables[0]}


def generate_graph(args, root_path, output_path):

    fixed_keys_values = [key + " : " + str(args[key]) for key in args.keys() if key not in ['derivatives']]

    derivatives_parsed, var_args = get_derivatives(args, root_path)

    var_key = var_args['var_key']
    variables = var_args['variables']

    graph_path = ", ".join(fixed_keys_values)

    graph_path = os.path.join(output_path, graph_path)

    if not os.path.isdir(graph_path):
        os.mkdir(graph_path)

    create_graph(args, var_key, variables, args['derivatives'], derivatives_parsed, graph_path)


if __name__ == '__main__':
    ROOT_EXPERIMENT_PATH = '/home/blackplague/IdeaProjects/query-caching/experiments'

    OUTPUT_PATH = '/home/blackplague/IdeaProjects/query-caching/analysis/findings'

    generate_graph({
        'experiment': ['fifo', 'rr'],
        # 'mode': ['sequence', 'hybrid', 'mvr'],
        'mode': 'batch',
        # 'cache_size': ['8', '64', '256', '512', '1024', '4096'],
        'cache_size': '2048',
        'query_type': 'filter_join_aggregate',
    }, ROOT_EXPERIMENT_PATH, OUTPUT_PATH)

    # experiment_types = ['fifo', 'mru']
    #
    #
    # experiment = 'fifo'
    # mode = 'sequence'
    # cache_sizes = ['4', '8']
    # query_type = 'all'
    # derivative = '10'
    #
    # all_derivatives = []
    #
    # for cache in cache_sizes:
    #     all_derivatives.push(build_experiment_graph(experiment, mode,cache, query_type, derivative))
    # pass
