import os
import copy


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

def build_experiment_graph(root_path, experiment, mode, cache_size, query_type, derivative):
    path = f'{root_path}/{experiment}'
    modes2 = [i for i in get_sub_dir_names(path) if i == mode]

    mode_paths = get_sub_dir_paths(path, modes2)

    for mode_path in mode_paths:

        cache_sizes = [i for i in get_sub_dir_names(mode_path) if i == cache_size]
        cache_paths = get_sub_dir_paths(mode_path, cache_sizes)

        for cache_path in cache_paths:
            query_types = [i for i in get_sub_dir_names(cache_path) if i == query_type]
            query_paths = get_sub_dir_paths(cache_path, query_types)

            for query_path in query_paths:
                dervibility_types = [i for i in get_sub_dir_names(query_path) if i == derivative]

                print(f'query_path: {query_path}')
                dervibility_paths = get_sub_dir_paths(query_path, dervibility_types)
                

                return DerivativeDirParser(
                    directory_paths=dervibility_paths,
                    experiment=experiment,
                    mode_type=get_child_name(mode_path),
                    cache_size=get_child_name(cache_path),
                    query_type=get_child_name(query_path),
                )._experiment_results[0]

    raise Exception("can not find derivative", f'experiment: {cache} mode: {mode} cache_size: {cache_size} query_type: {query_type} derivatives: {derivative}')

def get_derivatives(args, root_path, x_key, experiments):
    all_constants = []
    all_variables = []

    for key in args.keys():
        val = args[key]
        if key == x_key:
            all_variables.append(val)
        else:
            all_constants.append(val)

    all_experiment_results = []

    for experiment in experiments:
        args['experiment'] = experiment

        _temp = []

        for var in all_variables[0]:
            args[x_key] = var

            print(f'args: {args}')

            _temp.append(build_experiment_graph(root_path, **args))

        all_experiment_results.append(_temp)

    return all_experiment_results, {'var_key': x_key, 'variables': all_variables[0]}


def generate_graph(args, root_path, output_path, x_key, y_key):

    fixed_keys_values = [key + " : " + str(args[key]) for key in args.keys() if key != x_key]

    all_experiment_results, var_args = get_derivatives(copy.deepcopy(args), root_path, x_key, args['experiment'])

    var_key = var_args['var_key']
    variables = var_args['variables']

    graph_path = ", ".join(fixed_keys_values)

    graph_path = os.path.join(output_path, graph_path)

    if not os.path.isdir(graph_path):
        os.mkdir(graph_path)

    create_graph(
        args=args, 
        var_key=var_key, 
        variables=variables, 
        x_vars=args[y_key],
        y_vars=all_experiment_results, 
        graph_path=graph_path,
        experiments=args['experiment']
    )


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
    ROOT_EXPERIMENT_PATH = '/Users/new_horizon/query-caching/query-caching-results'

    OUTPUT_PATH = '/Users/new_horizon/query-caching/analysis/findings_1'

    generate_graph(
        {
            'experiment': [
                'lfu', 'fifo'
            ],
            'mode': 'hybrid',
            'cache_size': ['4', '32', '64', '128', '256', '512', '1024', '2048', '4096'],
            'query_type': 'all',
            'derivative': '75',
        }, ROOT_EXPERIMENT_PATH, OUTPUT_PATH,
        x_key='cache_size',
        y_key='experiment'
    )
