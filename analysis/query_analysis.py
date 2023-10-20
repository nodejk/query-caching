import glob


import matplotlib

font = {
        'size'   : 15}

matplotlib.rc('font', **font)

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors


if __name__ == '__main__':

    query_path = '/home/blackplague/docker-databases/mvo-tpc/tpc/dbgen/queries'

    output_dir = '/home/blackplague/IdeaProjects/query-caching/analysis/query_analysis'

    all_files = glob.glob(f'{query_path}/*.sql')

    all_tables = {
        "lineitem": 0,
        "orders": 0,
        "part": 0,
        "supplier": 0,
        "partsupp": 0,
        "customer": 0,
        "nation": 0,
        "region": 0,
    }

    for file in all_files:
        with open(file, 'r') as f:
            lines = f.readlines()

            for line in lines:

                processed_items = line.strip().split(' ')

                for item in processed_items:

                    processed_item = item.replace(',', '')

                    if processed_item in all_tables.keys():
                        all_tables[processed_item] += 1

    labels = []
    values = []

    for key in all_tables.keys():

        labels.append(key)
        values.append(all_tables[key])

        print(f'{key}, {all_tables[key]}')

    fig, ax_count = plt.subplots()

    width = 0.25
    ax_count.bar(x=labels, height = values)

    sorted_colors = sorted(
        mcolors.TABLEAU_COLORS
    )

    ax.set_yticks(ticks=plt.yticks()[0], labels=plt.yticks()[0].astype(int))

    ax.set_ylim(bottom=0, top=30)
    ax_count.set_ylabel("Number of queries")

    fig.set_tight_layout(True)
    # plt.subplots_adjust(left=0, bottom=0, right=1, top=1, wspace=0, hspace=0)
    plt.autoscale()
    ax.set_xticks(rotation=45)
    plt.savefig(f'{output_dir}/table_access_counts.png', dpi=120)

