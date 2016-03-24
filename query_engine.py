from utils import run_shell_cmd
from utils import fn_timer
import os
import logging
from multiprocessing import Pool

class Engine(object):



    def __init__(self, cmd, output, query_generator):

        self.cmd = cmd
        self.cmd_output = output
        self.perf_cmd = "/usr/bin/time -f \"%C,%e,%S,%U,%K\"  "
        self.query_generator = query_generator
        self.query_output = []


    @staticmethod
    def setup(cmd):
        '''
        Environment setup before running any tests

        :param cmd:
        :return:
        '''
        (output, err) = run_shell_cmd(cmd)
        if err:
            exit(1)

    @staticmethod
    def teardown(cmd):
        '''
        Clean-up after tests
        :param cmd:
        :return:
        '''
        (output, err) = run_shell_cmd(cmd)
        if err:
            exit(1)
        return



    def perf_run(self, num_of_time, average_results=True):
         self.query_output.append('Performance Query Results:')
         for f in os.listdir(self.query_generator.query_dir):
             if os.path.isfile(f):
                iterations = 0
                elapse = 0
                self.query_output.append('Running ' + f)
                for i in range(1, num_of_time):

                    run = self.perf_cmd + self.cmd + self.cmd_output
                    output, err = run_shell_cmd(run)
                    if not err:
                        iterations += 1
                        self.query_output.append(output)
                        elapse += output.split(',')[1]

                if average_results:
                    elapse = 0 if iterations == 0 else elapse / iterations
                    self.query_output.append( ' '.join(['average elapse:',  str(elapse)]) )
         return


    @fn_timer
    def concurrency_run(self):
        '''
         Currently not support error exit from sub-process
        '''
        self.query_output.append('Concurrency Query Test Stats:')
        pool_size = len(self.query_generator.queries)
        pool = Pool(processes = pool_size)
        results = pool.map_async(run_shell_cmd, self.query_generator.queries, pool_size)
        pool.close()
        pool.join()
        return 'concurrency returned in '




