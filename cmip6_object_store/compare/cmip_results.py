# Plotting and parsing code for cmip6 read tests
import pandas as pd
from pprint import pprint
import argparse
import os
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.style.use('seaborn-whitegrid')


parser = argparse.ArgumentParser(description='Gather variables from command line', formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument(
    'action',
    help = """ Actions allowed:
    print: print information
    remove: remove the correspoding records fro the results (makes a copy first)
    plot: plot results
        """,
    )

parser.add_argument(
    '--plot', '-p',
    dest='plot_type',
    required=False,
    help="Plot type:"
)

parser.add_argument(
    '--plot-name','-pn', 
    dest='plot_name',
    required=False,
    help="Plot name for save file",
    default="latest_plot.png"
)

parser.add_argument(
    '--var','-V',
    dest = "var",
    help = "Variable from the results file",
    )

parser.add_argument(
    '--value', '-v',
    dest = 'value',
    help="value from results file",
    action = "store",
    required=False
)

parser.add_argument(
    '--yscale','-y',
    dest="yscale",
    required=False,
    default='linear',
    help="yscale: linear | log"
)

parser.add_argument(
    '--selection', '-s',
    dest= 'sel',
    help= 'Selection for plotting',
    action= 'store',
    required= False,
    default= None
)


class CMIPParse:
    def __init__(self):
        self.data = pd.read_json('results_df.json')

    def _get_tests(self):
        tests = self.data['test']
        tests_set = set(tests)
        return tests_set
    
    def print_results(self, var):
        # get the right selection fromt the file
        self._check_var(var)
        tests = self._get_tests()
        
        print('Variable {} from results file:'.format(var))
        for t in tests:
            r_list = self.data[self.data['test'] == t][var].to_list()
            print('{} ({} repeats): {}'.format(t, len(r_list), r_list))

    def print_average_rate(self, test):
        self._check_test(test)
        rates = self.data[self.data['test']==test]


    def _check_test(self,test):
        # checks whether valid test been asked for
        tests = self.data['test']
        tests_set = set(tests)
        if test in tests_set:
            return True
        else:
            raise ValueError('{} not a valid test. Try one of: \n{}'.format(test,tests))
        
    def _check_var(self, var):
        # checks whether valid test been asked for
        keys = self.data.keys()
        if var in keys:
            return True
        else:
            raise ValueError('{} not a valid variable. Try one of: \n{}'.format(var,keys))

    def print_keys(self):
        # print the variables in the results frame
        keys= self.data.keys()
        print('Keys in data frame:')
        for k in keys:
            print(k)

    def print_tests(self):
        # print the test combinations from the results file
        tests = self.data['test']
        tests_set = set(tests)
        print('Tests available in the results file:')
        for i in tests_set:
            print(i)
    
    def remove_by_key(self,var,val):
        # removes the results from the results file which match the value to the variable key
        try:
            val = float(val)
        except ValueError:
            pass
        # make a copy
        os.system('cp results_df.json OLD_results_df.json')

        df = self.data[~self.data[var].isin([val])]
        df.to_json('results_df.json')

    def remove_by_pk(self,pk):
        # removes the results from the results file which match the value to the variable key
        # make a copy
        pk = int(pk)
        os.system('cp results_df.json OLD_results_df.json')

        df = self.data.drop(pk)
        df.to_json('results_df.json')

class CMIPPlot:
    def __init__(self):
        self.data = pd.read_json('results_df.json')

    def _get_tests(self, sel):
        if not sel:
            tests = self.data['test']
        else:
            tests = self.data[self.data['test'].str.contains(sel)]['test']
        tests_set = set(tests)
        return sorted(tests_set)

    def _boxplot(self, var, sel, yscale):
        # get the test names
        tests = self._get_tests(sel)

        results = []
        for t in tests:
            results.append(self.data[self.data['test']==t][var].to_list())

        plt.boxplot(results)
        plt.yscale(yscale)
        plt.xticks(range(1,len(tests)+1), tests)
        if var == 'rateMB':
            plt.ylabel('Rate (MB/s)')
        elif var == 'total_time':
            plt.ylabel('Time (s)')
        else: 
            plt.ylabel(var)

    def _scatterplot(self, var, sel, yscale):
        tests = self._get_tests(sel)
        ind_dict = {}
        for i in range(len(tests)):
            ind_dict[tests[i]] = i+1

        results = []
        xvals = []
        for t,v in zip(self.data['test'],self.data[var]):
            # check if the data line conforms to the sel var
            if not sel: 
                # if none append all results
                results.append(v)
                # get the index for the tes val
                xvals.append(ind_dict[t])
            else:
                if sel in t:
                    results.append(v)
                    # get the index for the tes val
                    xvals.append(ind_dict[t])

        plt.scatter(xvals,results)
        plt.yscale(yscale)
        plt.xticks(range(1,len(tests)+1), tests, rotation=10)
        if var == 'rateMB':
            plt.ylabel('Rate (MB/s)')
        elif var == 'total_time':
            plt.ylabel('Time (s)')
        else: 
            plt.ylabel(var)


    def plot(self, plot_type, sel, yscale):
        if plot_type == 'rate_boxplot':
            self._boxplot('rateMB', sel, yscale)
        elif plot_type == 'rate_scatter':
            self._scatterplot('rateMB', sel, yscale)
        elif plot_type == 'time_boxplot':
            self._boxplot('total_time', sel, yscale)
        elif plot_type == 'time_scatter':
            self._scatterplot('total_time', sel, yscale)
        else:
            raise ValueError('Bad config')

    def save_plot(self, fname):
        plt.savefig(fname)

    

if __name__ == "__main__":
    args = parser.parse_args()
    cmip_parse = CMIPParse()
    cmip_plot = CMIPPlot()

    if args.action == "print" and args.var == "keys":
        cmip_parse.print_keys()
    elif args.action == "print" and args.var == "tests":
        cmip_parse.print_tests()
    elif args.action == "print":
        cmip_parse.print_results(args.var)
    elif args.action == "remove":
        cmip_parse.remove_by_key(args.var, args.value)
    elif args.action == "removepk":
        cmip_parse.remove_by_pk(args.var)
    elif args.action == 'plot':
        cmip_plot.plot(args.plot_type, args.sel, yscale=args.yscale)
        cmip_plot.save_plot(args.plot_name)