import numpy as np

from serenity_pypeline.logger import log
from serenity_pypeline.db.important_consts import DATA_FIELD
from serenity_pypeline.filters.filter import Filter
from serenity_pypeline.filters.pca_finisher import PcaFinisher


class PcaExecutor(Filter):

    def __init__(self, conf):
        super(PcaExecutor, self).__init__(conf)

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

        log.info('Counting PCA... from ' + str(len(key_list)) + " dimensions.")

        corr_matrix = np.corrcoef(input_matrix)

        log.info(corr_matrix)
        return {PcaFinisher.KEY_FOR_DATA: self._prepare_data(key_list,
                                                     corr_matrix.tolist()),
                PcaFinisher.KEY_NAME: 'correlations'}

    def _post_effect_output(self, output, field):
        values = list()

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
                output[i]['value'] = 1.0

            values.append(output[i]['value'])

        if mismatches < 2:
            return list()

        return values

    def _prepare_data(self, key_list, corr_matrix):
        result = {}
        for key in key_list:
            index = key_list.index(key)
            result[key] = {}

            for val in corr_matrix[index]:
                val_index = corr_matrix[index].index(val)
                result[key][key_list[val_index]] = val

        log.info(result)
        return result

