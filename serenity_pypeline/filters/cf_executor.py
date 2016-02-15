import numpy as np

from sklearn.decomposition import FactorAnalysis
from serenity_pypeline.logger import log
from serenity_pypeline.db.important_consts import DATA_FIELD
from serenity_pypeline.filters.filter import Filter
from serenity_pypeline.filters.cf_finisher import CfFinisher


class CfExecutor(Filter):

    def __init__(self, conf):
        super(CfExecutor, self).__init__(conf)

    def run(self, **kwargs):
        input_matrix = []
        key_list = []
        data_to_test = kwargs[DATA_FIELD]

        spotted_length = -1
        for key, val in data_to_test.iteritems():
            points = list(val[0].get_points())
            values = self._post_effect_output(points, val[1])
            if len(values) == 0:
                log.error("No data points for measurement: " + str(key))
                continue

            log.debug("Last value in series:" + str(values[-1]))
            log.info(str(key) + " has length " + str(len(values)))
            if spotted_length == -1:
                spotted_length = len(values)
            elif spotted_length != len(values):
                raise ValueError("Measurements have different lengths!")

            input_matrix.append(values)
            key_list.append(key)

        log.info('Counting Common factor... from ' + str(len(key_list)) + " dimensions.")
        # Move to Common Factor! TODO:
        # corr_matrix = np.corrcoef(input_matrix)

        corrmat = np.corrcoef(input_matrix)
        corrmat = np.nan_to_num(corrmat)
        log.info(corrmat)
        eigenvalues, _ = np.linalg.eig(corrmat)
        eigenvalues = filter(lambda x: True if x > 1 else False, eigenvalues)
        eigenvalues = [x/len(key_list) for x in eigenvalues]

        input_matrix = np.array(input_matrix)
        input_matrix = np.transpose(input_matrix)
        factor = FactorAnalysis(n_components=len(eigenvalues)).fit(input_matrix)
        log.info(factor.components_)

        return {CfFinisher.KEY_FOR_DATA: self._prepare_data(key_list,
                                                             eigenvalues,
                                                             factor.components_),
                 CfFinisher.KEY_NAME: 'factors'}

    def _post_effect_output(self, output, field):
        values = list()
        # CF cannot count data from all 0 metrics.
        zeros = 0
        last_value = 0
        mismatches = 0
        for i in xrange(0, len(output)):
            output[i]['value'] = float(output[i]['value'])
            if i == 0:
                last_value = output[i]['value']
                continue
            elif last_value != output[i]['value']:
                mismatches += 1
                last_value = output[i]['value']


            if field["cumulated"]:
                output[i]['value'] -= output[i-1]['value']

            if field["mul"] != 1:
                output[i]['value'] *= field["mul"]

            if output[i]['value'] == 0.0:
                zeros += 1
                output[i]['value'] = 1.0

            values.append(output[i]['value'])

        if zeros >= (len(output)-2):
            return list()

        if mismatches < 2:
            return list()

        return values

    def _prepare_data(self, key_list, eigenvalues, factors):
        result = {}
        result['eigenvalues'] = eigenvalues
        result['factors'] = []

        for factor in factors:
            val = max(factor)
            i = list(factor).index(val)
            result['factors'].append(key_list[i])

        log.info(result)
        return result

