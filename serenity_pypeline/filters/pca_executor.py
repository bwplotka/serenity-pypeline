import numpy as np

from serenity_pypeline.logger import log
from serenity_pypeline.db.important_consts import DATA_FIELD
from serenity_pypeline.filters.filter import Filter


class PcaExecutor(Filter):

    def __init__(self, conf):
        super(PcaExecutor, self).__init__(conf)

    def run(self, **kwargs):
        data_to_test = kwargs[DATA_FIELD]
        log.info('Counting PCA...')

    def _calculate_corr_matrix(self, input_matrix):
        return np.corrcoef(input_matrix)
