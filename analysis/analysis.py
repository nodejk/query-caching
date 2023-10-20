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

from derivative_wise import create_graph_general

def build_experiment_graph(root_path, cache, mode, cache_size, query_type, derivative):

    path = f'{root_path}/{cache}'

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
                dervibility_types = [i for i in get_sub_dir_names(query_path) if i in derivative]
                dervibility_paths = get_sub_dir_paths(query_path, dervibility_types)

                return DerivativeDirParser(
                    directory_paths=dervibility_paths,
                    experiment=cache,
                    mode_type=get_child_name(mode_path),
                    cache_size=get_child_name(cache_path),
                    query_tye=get_child_name(query_path),
                )

    raise Exception("can not find derivative", f'experiment: {cache} mode: {mode} cache_size: {cache_size} query_type: {query_type} derivatives: {derivative}')

def get_derivatives(args, root_path):
    all_constants = []
    all_variables = []
    var_key = None

    for key in args.keys():
        val = args[key]

        if isinstance(val, list) and key != 'derivatives':
            var_key = key
            all_variables.append(val)
        else:
            all_constants.append(val)

    all_derivatives = []

    print(f'all_variables: {all_variables}')

    if len(all_variables) != 1:
        raise Exception('only one var allowed')

    proxy_args = args.copy()

    for var in all_variables[0]:
        proxy_args[var_key] = var
        all_derivatives.append(build_experiment_graph(root_path, **proxy_args))

    return all_derivatives, {'var_key': var_key, 'variables': all_variables[0]}

def get_double_derivatives(args, root_path):
    all_constants = []
    all_variables = []
    var_key = None

    for key in args.keys():
        val = args[key]

        if isinstance(val, list) and key != 'derivatives':
            var_key = key
            all_variables.append(val)
        else:
            all_constants.append(val)

    all_derivatives = []

    if len(all_variables) != 2:
        raise Exception('two vars allowed')

    proxy_args = args.copy()

    for var in all_variables[0]:
        proxy_args[var_key] = var
        all_derivatives.append(build_experiment_graph(root_path, **proxy_args))

    return all_derivatives, {'var_key': var_key, 'variables': all_variables[0]}
def generate_graph(args, root_path, output_path):

    fixed_keys_values = [key + " : " + str(args[key]) for key in args.keys() if key not in ['derivative']]

    derivatives_parsed, var_args = get_derivatives(args, root_path)

    var_key = var_args['var_key']
    variables = var_args['variables']

    graph_path = ", ".join(fixed_keys_values)

    graph_path = os.path.join(output_path, graph_path)

    if not os.path.isdir(graph_path):
        os.mkdir(graph_path)

    create_graph(args, var_key, variables, args['derivatives'], derivatives_parsed, graph_path)


def generate_graph_general(args, root_path, output_path):

    fixed_keys_values = [key + " : " + str(args[key]) for key in args.keys()]

    derivatives_parsed, var_args = get_derivatives(args, root_path)

    var_key = var_args['var_key']
    variables = var_args['variables']

    graph_path = ", ".join(fixed_keys_values)

    graph_path = os.path.join(output_path, graph_path)

    if not os.path.isdir(graph_path):
        os.mkdir(graph_path)

    print(f'variables: {variables}, {var_args}, {[i.__str__() for i in derivatives_parsed]}')

    create_graph_general(
        args=args,
        var_key=var_key,
        y_variables=variables,
        x_variables=derivatives_parsed,
        graph_path=graph_path,
        y_label="Time (s)",
        x_label= "Cache Size (MB)",
    )

def generate_graph_derivative_separately(args, root_path, output_path):
    fixed_keys_values = [key + " : " + str(args[key]) for key in args.keys() if key not in ['derivatives']]

    derivatives = args['derivatives']

    derivatives_parsed, var_args = get_derivatives(args, root_path)

    derivative_mapping = {i: [] for i in derivatives}

    print(derivative_mapping)

    for der in derivatives:

        val_derivative = []

        print('derivatives_parsed', type(derivatives_parsed))
        for variable_der in derivatives_parsed:
            for curr_der in variable_der.get_experiment_results():
                if curr_der.get_derivative() == der:
                    val_derivative.append(curr_der)






if __name__ == '__main__':
    ROOT_EXPERIMENT_PATH = '/home/blackplague/IdeaProjects/query-caching/experiments'

    OUTPUT_PATH = '/home/blackplague/IdeaProjects/query-caching/analysis/findings'

    # generate_graph({
    #     'experiment': 'mru',
    #     # 'mode': ['sequence', 'hybrid', 'mvr'],
    #     'mode': 'sequence',
    #     # 'cache_size': ['8', '32', '64', '256', '512', '1024'],
    #     'cache_size': ['4', '32', '64', '128', '256'],
    #     'query_type': 'all',
    #     # 'query_type': ['all', 'complex_filter', 'filter_join'],
    #     'derivative': ['10', '25', '45', '75', '90'],
    # }, ROOT_EXPERIMENT_PATH, OUTPUT_PATH)

    generate_graph_general({
        'cache': 'mru',
        # 'mode': ['sequence', 'hybrid', 'mvr'],
        'mode': 'sequence',
        # 'cache_size': ['8', '32', '64', '256', '512', '1024'],
        'cache_size': ['4', '32', '64', '128', '256'],
        'query_type': 'all',
        # 'query_type': ['all', 'complex_filter', 'filter_join'],
        'derivative': '10',
    }, ROOT_EXPERIMENT_PATH, OUTPUT_PATH)
    # generate_graph_derivative_separately({
    #     'experiment': ['rr', 'mru'],
    #     # 'mode': ['sequence', 'hybrid', 'mvr'],
    #     'mode': 'mvr',
    #     # 'cache_size': ['8', '64', '256', '512', '1024', '4096'],
    #     'cache_size': '2048',
    #     'query_type': 'all',
    #     'derivative': ['10', '25', '45', '75', '90'],
    # }, ROOT_EXPERIMENT_PATH, OUTPUT_PATH)

    # experiment_types = ['fifo', 'mru']
    #
    #
    # experiment = 'fifo'
    # mode = 'sequence'
    # cache_sizes = ['4', '8']
    # query_type = 'all'
    # derivative = '10'
    #
    # all_derivative = []
    #
    # for cache in cache_sizes:
    #     all_derivative.push(build_experiment_graph(experiment, mode,cache, query_type, derivative))
    # pass
