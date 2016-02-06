import numpy as np

from serenity_pypeline.logger import log
from serenity_pypeline.db.important_consts import DATA_FIELD
from serenity_pypeline.filters.filter import Filter


class PcaExecutor(Filter):

    def __init__(self, conf):
        super(PcaExecutor, self).__init__(conf)

    def run(self, **kwargs):
        log.info('Counting PCA...')

        input_matrix = []
        data_to_test = kwargs[DATA_FIELD]
        for key, val in data_to_test.iteritems():
            g = val.get_points()
            l = list(g)
            log.info(str(key) + " has length " + str(len(l)))
            l = [x['value'] for x in l]
            input_matrix.append(l)

        corr_matrix = np.corrcoef(input_matrix)

        log.info(corr_matrix)
        return corr_matrix
