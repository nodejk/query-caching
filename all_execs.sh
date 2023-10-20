# Sequential

declare -a cache_type=("mru" "rr" "lru" "fifo" "lfu")
declare -a mode_type=("sequence" "hybrid" "batch" "mvr")
declare -a cache_sizes=("4" "8" "16" "32" "64" "128" "256" "512" "1024" "2048" "4096")
declare -a query_type=("all" "complex_filter" "filter_join" "filter_aggregate" "filter_join_aggregate")
declare -a derivibility_percentages=("10" "25" "45" "75" "90")
declare -a dimension_types=("size_bytes")
#for type in {1..5}; do
#  for der in {0..12}; do
#    for cache in "${arr[@]}"; do
#      mkdir("$pwd/experiments/$cache")
#      echo "SEQ: Derivability: $der Type: $type, Cache type: $cache"
#      sudo ./gradlew --stop && ./gradlew run --args="0 0 $der $type" > seq\_0\_$der\_$type.txt
#  #    echo "floating by the docks" | sudo -S service postgresql restart
#    done
#  done
#done

  #psql -U root -> create database root;
#psql databasename < data_base_dump

for cache in "${cache_type[@]}"; do
  for mode in "${mode_type[@]}"; do
    for cache_size in "${cache_sizes[@]}"; do
      for query in "${query_type[@]}"; do
        for derivibility in "${derivibility_percentages[@]}"; do
          for dimension in "${dimension_types[@]}"; do

            CACHE_FOLDER="./experiments/$cache"
            if [ ! -d $CACHE_FOLDER ]; then
              mkdir -p $CACHE_FOLDER;
            fi

            MODE_FOLDER="$CACHE_FOLDER/$mode"
            if [ ! -d $MODE_FOLDER ]; then
              mkdir -p $MODE_FOLDER;
            fi

            CACHE_SIZE_FOLDER="$MODE_FOLDER/$cache_size"
            if [ ! -d $CACHE_SIZE_FOLDER ]; then
              mkdir -p $CACHE_SIZE_FOLDER;
            fi

            QUERY_FOLDER="$CACHE_SIZE_FOLDER/$query"
            if [ ! -d QUERY_FOLDER ]; then
              mkdir -p QUERY_FOLDER;
            fi

            DERIVIABILITY="$QUERY_FOLDER/$derivibility"
            if [ ! -d $DERIVIABILITY ]; then
              mkdir -p $DERIVIABILITY;
            fi

            FINAL_FOLDER="$DERIVIABILITY/$dimension"
            if [ ! -d $FINAL_FOLDER ]; then
              mkdir -p $FINAL_FOLDER;
            fi

            echo "
            -------------------------------------------
            |        CACHE_TYPE: $cache               |
            |        MODE: $mode                      |
            |        CACHE_SIZE: $cache_size          |
            |        QUERY_TYPE: $query               |
            |        DERIVIVIBILITY: $derivibility    |
            |        DIMENSION: $dimension            |
            -------------------------------------------
            "

#            sudo docker compose -f /home/blackplague/docker-databases/mvo-tpc/docker-compose.yaml up
#              java -jar build/libs/mqo-1.0-SNAPSHOT.jar $cache $mode $cache_size $query $derivibility $dimension
             ./gradlew run --args="$cache $mode $cache_size $query $derivibility $dimension" --no-watch-fs --warning-mode=none > "$FINAL_FOLDER/result.txt"

#            gradle run --args="$cache $mode $cache_size $query $derivibility $dimension" > "$FINAL_FOLDER/result.txt"
              sudo -S service postgresql restart

#            sudo docker exec -it mvo-tpc-vm-1 psql -U root -c 'SELECT pg_reload_conf()';
          done
        done
      done
    done
  done
done
#gradle  run --args="fifo mvr 16 all 40 size_bytes"

## Hybrid
#for type in {1..5}; do
#  for cache in {0..9}; do
#    for der in {0..12}; do
#      echo "HYB: Derivability: $der, Cache: $cache, Type: $type"
#      sudo ./gradlew --stop && ./gradlew run --args="1 $cache $der $type" > hyb\_$cache\_$der\_$type.txt
##      echo "floating by the docks" | sudo -S service postgresql restart
#    done
#  done
#done
##
### Batch
#for type in {1..5}; do
#  for der in {0..12}; do
#    echo "BAT: Derivability: $der, Type: $type"
#    sudo ./gradlew --stop && ./gradlew run --args="2 0 $der $type" > bat\_0\_$der\_$type.txt
##    echo "floating by the docks" | sudo -S service postgresql restart
#  done
#done
##
### MVR
#for type in {1..5}; do
#  for cache in {0..9}; do
#    for der in {0..12}; do
#      echo "MVR: Derivability: $der, Cache: $cache, Type: $type"
#      sudo ./gradlew --stop && ./gradlew run --args="3 $cache $der $type" > mvr\_$cache\_$der\_$type.txt
##      echo "floating by the docks" | sudo -S service postgresql restart
#    done
#  done
#done