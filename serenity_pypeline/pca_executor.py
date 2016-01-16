from filter_step import FilterStep
import numpy as np

__author__ = 'MacRomanowski'


class PcaExecutor(FilterStep):

    def __init__(self, conf):
        super(FilterStep, self).__init__(conf)

    def run(self, **kwargs):
        # TODO: implement it
        print 'Counting PCA...'

    def _calculate_corr_matrix(self, input_matrix):
        return np.corrcoef(input_matrix)
