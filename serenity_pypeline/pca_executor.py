from filter_step import FilterStep
import numpy as np
from important_consts import DATA_FIELD

__author__ = 'MacRomanowski'


class PcaExecutor(FilterStep):

    def __init__(self, conf):
        super(FilterStep, self).__init__()

    def run(self, **kwargs):
        data_to_test = kwargs[DATA_FIELD]
        print 'Counting PCA...'

    def _calculate_corr_matrix(self, input_matrix):
        return np.corrcoef(input_matrix)
