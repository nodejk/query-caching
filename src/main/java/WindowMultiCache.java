import batch.QueryBatcher;
import batch.data.BatchedQuery;
import cache.enums.DimensionType;
import cache.models.AbstractCachePolicy;
import common.*;
import enums.Mode;
import enums.QueryType;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import test.QueryProvider;
import utils.CacheBuilder;
import utils.Configuration;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static common.Logger.logError;
import static common.Logger.logTime;
import static common.Utils.humanReadable;

public class WindowMultiCache {
    private final QueryExecutor executor;
    private final QueryProvider provider;

    private final QueryBatcher batcher;
    private final MViewOptimizer optimizer;
    private final AbstractCachePolicy<RelOptMaterialization> cache;

    //This tracks the time taken to calculate the size of the materialized view
    //so that we can subtract this from the execution time.
    public long subtractable;

    private Configuration configuration;

    public WindowMultiCache(
            CalciteConfiguration calciteConfiguration,
            QueryType queryType,
            Configuration configuration
    ) {
        this.executor = new QueryExecutor(calciteConfiguration);
        this.provider = new QueryProvider(queryType);

        this.batcher = new QueryBatcher(calciteConfiguration, executor);
        this.optimizer = new MViewOptimizer(calciteConfiguration);

        this.configuration = configuration;

        CacheBuilder<RelOptMaterialization> cacheBuilder = new CacheBuilder<>();

        this.cache = cacheBuilder.build(configuration);
    }

    public void run(Mode mode) {
        int count = 0, numQueries = 0;

        final long t1 = System.currentTimeMillis();
        for (List<String> qs : this.provider.queries) {
            System.out.println("===============================================");
            System.out.printf("%d: (%d)\nNo. of queries: %d\n", count, numQueries, qs.size());

            if (count % 5 == 0) {
                long time = System.currentTimeMillis() - t1 - subtractable - CustomPlanner.diff;
                logTime(String.format("Executed: %d: (%d) in %d ms", count, numQueries, time));
            }

            count += 1;
            numQueries += qs.size();

            if (mode == Mode.SEQUENCE) runSequentially(qs);
            else if (mode == Mode.HYBRID) handle(qs);
            else if (mode == Mode.BATCH) runInBatchMode(qs);
            else if (mode == Mode.MVR) runInMVRMode(qs);
        }

        long time = System.currentTimeMillis() - t1 - subtractable;
        logTime("Stopping... Time: " + time + " ms, Time no sub: " + (System.currentTimeMillis() - t1) + " ms");
    }

    private void runSequentially(List<String> queries) {
        for (String query : queries) {
            executor.execute(executor.getLogicalPlan(query), null);
        }
    }

    private void runInBatchMode(List<String> queries) {
        System.out.println("Batching queries:");
        for (String query : queries) {
            System.out.println(Utils.getPrintableSql(query) + "\n");
        }

        List<BatchedQuery> batched = batcher.batch(queries);

        // Find out all the queries from the list that couldn't be batched and run them individually
        List<Integer> batchedIndexes = batched.stream().flatMap(bq -> bq.indexes.stream()).collect(Collectors.toList());
        List<Integer> unbatchedIndexes = IntStream.range(0, queries.size()).boxed().collect(Collectors.toList());
        unbatchedIndexes.removeAll(batchedIndexes);
        for (int i : unbatchedIndexes) {
            executor.execute(executor.getLogicalPlan(queries.get(i)), null);
        }

        for (BatchedQuery bq : batched) {
            System.out.println();
//            System.out.println("Batched SQL: " + Utils.getPrintableSql(bq.sql));
            System.out.println();
            SqlNode validated = executor.validate(bq.sql);
            RelNode plan = executor.getLogicalPlan(validated);

            RelOptMaterialization materialization = optimizer.materialize(bq.sql, plan);

            for (SqlNode partQuery : bq.parts) {
                RelNode logicalPlan = executor.getLogicalPlan(partQuery);
                RelNode partSubstitutable = materialization != null
                        ? getSubstitution(materialization, logicalPlan)
                        : getSubstitution(partQuery, logicalPlan);

                if (partSubstitutable == null) {
                    logError("This shouldn't happen!!!!!! Batch query is substitutable but parts are not. Exec query normally");
                    executor.execute(logicalPlan, rs -> System.out.println("Executed " + partQuery.toString()));
                    continue;
                }
                executor.execute(partSubstitutable, rs -> System.out.println("MVS Part Executed " + bq.sql));
                System.out.println();
            }
        }

    }

    private void runInMVRMode(List<String> queries) {
        for (String query: queries) {
            runIndividualQuery(query);
        }
    }

    private void handle(List<String> queries) {
        if (queries.size() == 1) {
            runIndividualQuery(queries.get(0));
        } else {
            runBatchQueries(queries);
        }
    }

    Random r = new Random(141221);

    //TODO: Move canonicalize outside the loop
    private RelNode getSubstitution(SqlNode validated, RelNode logicalPlan) {
        String key = getKey(validated);
        List<RelOptMaterialization> possibles = this.cache.get(key);

        String[] spl = StringUtils.splitByWholeSeparator(key, ",");
        for (String splPart : spl) {
            possibles.addAll(this.cache.get(splPart));
        }

//        System.out.println("--[FOUND_POSSIBLE_SUBTITUTIONS] \n-------" + possibles);

        for (RelOptMaterialization materialization : possibles) {
            RelNode substituted = this.optimizer.substitute(materialization, logicalPlan);

            if (substituted != null) {
                if (
                        this.configuration.lowerDerivability == 0 ||
                                r.nextDouble() > this.configuration.lowerDerivability
                ) {
                    System.out.println();
                    System.out.println(this.configuration.lowerDerivability == 0 ? "DIR RET" : "COMP");
                    System.out.println();
//                    System.exit(1);
                    return substituted;
                }
            }
        }
        return null;
    }

    private String getKey(SqlNode validated) {
        return String.join(",", QueryUtils.from(validated));
    }

    private RelNode getSubstitution(RelOptMaterialization materialization, RelNode logicalPlan) {
        return optimizer.substitute(materialization, logicalPlan);
    }

    private void runIndividualQuery(String q) {
//        System.out.println("[NORMAL_EXEC] \n" + Utils.getPrintableSql(q));

        SqlNode validated = this.executor.validate(q);
        RelNode logicalPlan = this.executor.getLogicalPlan(validated);
        RelNode substituted = this.getSubstitution(validated, logicalPlan);

        if (substituted == null) {
//            System.out.println("[CREATING_MV] \n " + Utils.getPrintableSql(q) + "\n");
            RelOptMaterialization materialization = optimizer.materialize(q, logicalPlan);

            long t1 = System.currentTimeMillis();
            long value = this.cache.dimension.getType() == DimensionType.SIZE_BYTES
                    ? QueryUtils.getTableSize2(q, materialization, executor)
                    : 1;

            subtractable += (System.currentTimeMillis() - t1);
            logTime("Calculating table size took " + (System.currentTimeMillis() - t1) + " ms, Size:" + humanReadable(value));

            String key = this.getKey(validated);

            this.cache.add(key, materialization, value);
            //TODO: Profile this, is this executed again? If so, find a way to extract results from
            //TODO: materialized table
            executor.execute(getSubstitution(materialization, logicalPlan), rs -> System.out.println("Executed " + q.replace("\n", " ")));
        } else {
            executor.execute(substituted, rs -> System.out.println("MVS Executed " + q.replace("\n", " ")));
        }
    }

    private void runBatchQueries(List<String> queries) {
        for (int i = queries.size() - 1; i >= 0; i--) {
            SqlNode validated = executor.validate(queries.get(i));
            RelNode logical = executor.getLogicalPlan(validated);
            RelNode substituted = getSubstitution(validated, logical);
            if (substituted != null) {
                executor.execute(substituted, rs -> System.out.println("OOB Executed"));
                queries.remove(i);
            }
        }

        for (String query : queries) {
            System.out.println(Utils.getPrintableSql(query) + "\n");
        }

        List<BatchedQuery> batched = batcher.batch(queries);

        // Find out all the queries from the list that couldn't be batched and run them individually
        List<Integer> batchedIndexes = batched.stream().flatMap(bq -> bq.indexes.stream()).collect(Collectors.toList());
        List<Integer> unbatchedIndexes = IntStream.range(0, queries.size()).boxed().collect(Collectors.toList());
        unbatchedIndexes.removeAll(batchedIndexes);
        for (int i : unbatchedIndexes) {
            runIndividualQuery(queries.get(i));
        }

        // Execute batched queries
        // For each batched query find out if any materialized view can be used
        // If not, then execute the batch queries individually
        // If yes, then it means that the batch query parts can also use that same MV
        // Find substitutions and execute
        for (BatchedQuery bq : batched) {
            System.out.println();
//            System.out.println("Batched SQL: " + Utils.getPrintableSql(bq.sql));
            System.out.println();
            SqlNode validated = executor.validate(bq.sql);
            RelNode plan = executor.getLogicalPlan(validated);
            RelNode substitutable = getSubstitution(validated, plan);
            RelOptMaterialization materialization = null;
            if (substitutable == null) {
                materialization = this.optimizer.materialize(bq.sql, plan);

                long t1 = System.currentTimeMillis();
                long value = this.cache.dimension.getType() == DimensionType.SIZE_BYTES
                        ? QueryUtils.getTableSize2(bq.sql, materialization, executor)
                        : 1;
                subtractable += (System.currentTimeMillis() - t1);
                logTime("Calculating table size took " + (System.currentTimeMillis() - t1) + " ms, Size:" + humanReadable(value));

                String key = this.getKey(validated);

                this.cache.add(key, materialization, value);
            }

            for (SqlNode partQuery : bq.parts) {
                RelNode logicalPlan = this.executor.getLogicalPlan(partQuery);
                System.out.println("logicalPlan---> " + logicalPlan.toString());
                RelNode partSubstitutable = materialization != null
                        ? this.getSubstitution(materialization, logicalPlan)
                        : this.getSubstitution(partQuery, logicalPlan);
                System.out.println("2. PARTIAL_SUBS--->" + partQuery.toString());
                if (partSubstitutable == null) {
                    logError("This shouldn't happen!!!!!! Batch query is substitutable but parts are not. Exec query normally");
                    this.executor.execute(logicalPlan, rs -> System.out.println("Executed " + partQuery.toString()));
                    continue;
                }
                this.executor.execute(partSubstitutable, rs -> System.out.println("MVS Part Executed " + bq.sql));
                System.out.println();
            }
        }
    }
}
