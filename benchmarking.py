from query_generator import *
from impala_engine import *
from hive_engine import *
from drill_engine import *
import logging
import os


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                   format='%(asctime)s %(name)-8s %(levelname)-5s %(message)s',
                   datefmt='%m-%d %H:%M',
                   filename=__file__ + '.log',
                   filemode='w')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)



    # Performance testing for hive
    logging.info("Starting performance testing for Hive...")
    query_generator = QueryGenerator('', 'sql/hive')
    hive_engine = HiveEngine('hive -S -f  ', ' 2>&1 1>/dev/null', query_generator)
    # turn off system cache
    #hive_engine.setup('sudo echo 1 > /proc/sys/vm/drop_caches ')
    hive_engine.perf_run(2)
    hive_engine.report()

    # Performance testing for impala
    logging.info("Starting performance testing for Impala...")
    query_generator = QueryGenerator('', 'sql/impala')
    impala_engine = ImpalaEngine('impala-shell -B -f  ', ' -o impala.test.out 2>&1', query_generator)
    impala_engine.perf_run(2)
    impala_engine.report()

    # Performance testing for drill
    logging.info("Starting performance testing for Drill...")
    query_generator = QueryGenerator('', 'sql/drill')
    drill_engine = DrillEngine('sqlline -u  sqlline -u jdbc:drill:zk=ip-10-9-1-197:5181,ip-10-9-1-198:5181,ip-10-9-1-196:5181,ip-10-9-1-124:5181  --fastConnect=true --silent=true --showHeader=false -f ', ' 2>&1 1>/dev/null', query_generator)
    drill_engine.perf_run(2)
    drill_engine.report()


    # Concurrency testing for hive
    logging.info("Starting concurrency testing for Hive...")
    query_generator = QueryGenerator('sql/hive/con/test', 'sql/hive')
    query_generator.parse_definition()
    hive_engine = HiveEngine('ls ', '', query_generator)
    elapsed = hive_engine.concurrency_run()
    logging.info(elapsed)

    # Concurrency testing for impala
    logging.info("Starting concurrency testing for Impala...")
    query_generator = QueryGenerator('sql/impala/con/test', 'sql/impala')
    query_generator.parse_definition()
    impala_engine = ImpalaEngine('ls ', '', query_generator)
    elapsed = impala_engine.concurrency_run()
    logging.info(elapsed)

    # Concurrency testing for hive
    logging.info("Starting concurrency testing for Drill...")
    query_generator = QueryGenerator('sql/drill/con/test', 'sql/drill')
    query_generator.parse_definition()
    drill_engine = DrillEngine('ls ', '', query_generator)
    elapsed = drill_engine.concurrency_run()
    logging.info(elapsed)


    #hive_engine.teardown('sudo echo 0 > /proc/sys/vm/drop_caches ')
