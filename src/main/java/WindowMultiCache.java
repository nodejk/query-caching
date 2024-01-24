import batch.QueryBatcher;
import batch.data.BatchedQuery;
import cache.enums.DimensionType;
import cache.models.AbstractCachePolicy;
import cache.models.CacheItem;
import common.*;
import enums.Mode;
import enums.QueryType;
import kotlin.Pair;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import test.QueryProvider;
import utils.CacheBuilder;
import utils.Configuration;
import utils.WindowRunUtils;
import utils.WindowRunUtilsList;

import java.util.LinkedList;
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

    private WindowRunUtilsList windowRunUtilsList;

    /**
     * just measure the average for each experiment
     * apache-wayang:
     *
     * comparing caching techniques for mqo
     */
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

        this.windowRunUtilsList = new WindowRunUtilsList();

        this.cache = cacheBuilder.build(configuration);
    }

    public void run(Mode mode) {
        int count = 0, numQueries = 0;

        final long t1 = System.currentTimeMillis();

        for (int qIdx = 0; qIdx < this.provider.queries.size(); qIdx++) {
            System.out.println(String.format("qIdx: %d/%d", qIdx + 1, this.provider.queries.size()));

            List<String> qs = this.provider.queries.get(qIdx);

            try {
                long startTime = System.currentTimeMillis();

                System.out.println("START==============================================");

                int count_q = 0;
                System.out.println("<QUERIES>");
                for (String q: qs) {
                    System.out.println(count_q + ": " + q);

                    count_q++;
                }
                System.out.println("</QUERIES>");

                WindowRunUtils tempUtils = new WindowRunUtils();

                System.out.printf("\n[BATCH] %d: (%d)\nNo. of queries: %d\n", count, numQueries, qs.size());

                if (count % 5 == 0) {
                    long time = System.currentTimeMillis() - t1 - subtractable - CustomPlanner.diff;
                    logTime(String.format("Executed: %d: (%d) in %d ms", count, numQueries, time));
                }

                count += 1;
                numQueries += qs.size();

                tempUtils.incrementNumberOfQueries(numQueries);

                if (mode == Mode.SEQUENCE) this.runSequentially(qs, tempUtils);
                else if (mode == Mode.HYBRID) this.handle(qs, tempUtils);
                else if (mode == Mode.BATCH) this.runInBatchMode(qs, tempUtils);
                else if (mode == Mode.MVR) this.runInMVRMode(qs, tempUtils);

                System.out.println(tempUtils);

                System.out.println("\n[EXEC]-" +
                        String.format("{ \"num_queries\": \"%d\", \"time_taken\": \"%d\" }",
                                qs.size(),
                                (System.currentTimeMillis() - startTime)
                        )
                );

                this.windowRunUtilsList.addNewItem(tempUtils);

            } catch (Exception | Error e) {
                logError("{\"CANNOT_RUN_QUERIES\": " + qs + "}");
            }

        }

        long time = System.currentTimeMillis() - t1 - subtractable;
        logTime("Stopping... Time: " + time + " ms, Time no sub: " + (System.currentTimeMillis() - t1) + " ms");

        System.out.println(this.cache.cacheHitString());
    }

    private void runSequentially(List<String> queries, WindowRunUtils windowRunUtils) {

        for (String query : queries) {
            try {
                this.executor.execute(this.executor.getLogicalPlan(query), null);
            } catch (Exception e) {
                continue;
            }
        }
    }

    private void runInBatchMode(List<String> queries, WindowRunUtils utils) {
        System.out.println("Batching queries:");

        for (String query : queries) {
            System.out.println(Utils.getPrintableSql(query) + "\n");
        }

        List<BatchedQuery> batched = this.batcher.batch(queries);

        // Find out all the queries from the list that couldn't be batched and run them individually
        List<Integer> batchedIndexes = batched.stream().flatMap(bq -> bq.indexes.stream()).collect(Collectors.toList());
        List<Integer> unbatchedIndexes = IntStream.range(0, queries.size()).boxed().collect(Collectors.toList());
        unbatchedIndexes.removeAll(batchedIndexes);

        for (int i : unbatchedIndexes) {
            executor.execute(executor.getLogicalPlan(queries.get(i)), null);
        }

        for (BatchedQuery bq : batched) {
            System.out.println();

            System.out.println("Batched SQL: " + Utils.getPrintableSql(bq.sql));

            System.out.println();
            SqlNode validated = executor.validate(bq.sql);
            RelNode plan = executor.getLogicalPlan(validated);

            RelOptMaterialization materialization = optimizer.materialize(bq.sql, plan);

            for (SqlNode partQuery : bq.parts) {
                RelNode logicalPlan = executor.getLogicalPlan(partQuery);
                RelNode partSubstitutable = materialization != null
                        ? this.getSubstitution(materialization, logicalPlan)
                        : this.getSubstitution(partQuery, logicalPlan, utils);

                if (partSubstitutable == null) {
                    logError("This shouldn't happen!!!!!! Batch query is substitutable but parts are not. Exec query normally");

                    System.out.println("Executing... " + partQuery.toString());
                    try {
                        executor.execute(logicalPlan, rs -> System.out.println("Executed " + partQuery.toString()));
                    } catch (Exception e) {
                        System.out.println("CAN NOT EXECUTE QUERY");

                    }
                    continue;
                }

                executor.execute(partSubstitutable, rs -> System.out.println("MVS Part Executed " + bq.sql));
                System.out.println();
            }
        }

    }

    private void runInMVRMode(List<String> queries, WindowRunUtils utils) {
        for (String query: queries) {
            this.runIndividualQuery(query, utils);
        }
    }

    private void handle(List<String> queries, WindowRunUtils utils) {
        if (queries.size() == 1) {
            this.runIndividualQuery(queries.get(0), utils);
        } else {
            this.runBatchQueries(queries, utils);
        }
    }

    Random r = new Random(141221);

    //TODO: Move canonicalize outside the loop
    private RelNode getSubstitution(SqlNode validated, RelNode logicalPlan, WindowRunUtils utils) {
        String key = this.getKey(validated);

        System.out.println("Substitution: " + validated.toString());
        List<Pair<String, RelOptMaterialization>> possibles = new LinkedList<>();

        this.cache.getAllItemsForRead(key).stream().forEach((RelOptMaterialization item) -> {
           possibles.add(new Pair<>(key, item));
        });

        String[] spl = StringUtils.splitByWholeSeparator(key, ",");
        for (String splPart : spl) {


            this.cache.getAllItemsForRead(splPart).stream().forEach((RelOptMaterialization item) -> {
                possibles.add(new Pair<>(key, item));
            });


        }

//        System.out.println("--[FOUND_POSSIBLE_SUBSTITUTIONS] \n-------" + possibles);

        for (Pair<String, RelOptMaterialization> item : possibles) {

            String _key = item.getFirst();
            RelOptMaterialization materialization = item.getSecond();


            RelNode substituted = this.optimizer.substitute(materialization, logicalPlan);

            if (substituted != null) {
                if (
                        this.configuration.lowerDerivability == 0 ||
                        r.nextDouble() > this.configuration.lowerDerivability
                ) {
                    System.out.println();
                    System.out.println(this.configuration.lowerDerivability == 0 ? "DIR RET" : "COMP");

                    this.cache.get(_key);
                    utils.incrementNumberTotalCacheHitByOne();
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

    private void runIndividualQuery(String q, WindowRunUtils utils) {

        SqlNode validated = this.executor.validate(q);
        RelNode logicalPlan = this.executor.getLogicalPlan(validated);
        RelNode substituted = this.getSubstitution(validated, logicalPlan, utils);

        if (substituted == null) {
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
            executor.execute(this.getSubstitution(materialization, logicalPlan), rs -> System.out.println("Executed " + q.replace("\n", " ")));
        } else {

            long startTime = System.currentTimeMillis();

            System.out.println("substituted: " + substituted.toString());

            executor.execute(substituted, rs -> System.out.println("MVS Executed " + q.replace("\n", " ")));

            System.out.println("TIME FOR EXECUTING SINGLE QUERY: " + (System.currentTimeMillis() - startTime));
        }
    }

    private void runBatchQueries(List<String> queries, WindowRunUtils utils) {
        for (int i = queries.size() - 1; i >= 0; i--) {
            SqlNode validated = this.executor.validate(queries.get(i));
            RelNode logical = this.executor.getLogicalPlan(validated);

            RelNode substituted = this.getSubstitution(validated, logical, utils);

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

        System.out.println("FOUND : " + unbatchedIndexes.size() + " unbatchable queries");

        for (int i : unbatchedIndexes) {
            this.runIndividualQuery(queries.get(i), utils);
        }

        // Execute batched queries
        // For each batched query find out if any materialized view can be used
        // If not, then execute the batch queries individually
        // If yes, then it means that the batch query parts can also use that same MV
        // Find substitutions and execute
//      //
        long startTime = System.currentTimeMillis();

        for (BatchedQuery bq : batched) {
            System.out.println();
            System.out.println();
            SqlNode validated = executor.validate(bq.sql);
            RelNode plan = executor.getLogicalPlan(validated);

            RelNode substitutable = this.getSubstitution(validated, plan, utils);

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
                RelNode partSubstitutable = materialization != null
                        ? this.getSubstitution(materialization, logicalPlan)
                        : this.getSubstitution(partQuery, logicalPlan, utils);

                if (partSubstitutable == null) {
                    logError("This shouldn't happen!!!!!! Batch query is substitutable but parts are not. Exec query normally");

                    try {
                        this.executor.execute(logicalPlan, rs -> System.out.println("Executed " + partQuery.toString()));
                    } catch (Exception e) {
                        continue;
                    }
                    continue;
                }
                this.executor.execute(partSubstitutable, rs -> System.out.println("MVS Part Executed " + bq.sql));
                System.out.println();
            }
        }

        long totalTimeTaken = System.currentTimeMillis() - startTime;
    }
}
