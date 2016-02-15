from serenity_pypeline.logger import log
from serenity_pypeline.db.influxdb_connector import InfluxDbConnector
from serenity_pypeline.db.important_consts import DATABASE_METRICS_CONF_FILE, \
    SERENITY_METRICS_CONF_FILE, DATA_FIELD

from sqlbuilder.smartsql import Q, compile
from serenity_pypeline.filters.filter import Filter

TIME_FROM_NOW = "1h"

class MetricsFetcher(Filter):

    def __init__(self, conf):
        super(MetricsFetcher, self).__init__(conf)

        # TODO: type of the database engine
        # should be loaded from the workflow configuration file
        self._dbConnector = InfluxDbConnector(conf)
        self._dbConnector.connect()
        self._result = None
        self.source = conf['default']['node']

    def run(self, **kwargs):
        self._result = kwargs

        # data filtered from Serenity input...
        data_to_test = self._filter_data_from_serenity(kwargs)

        # ... will be updated by database data
        metrics_to_query = self._get_which_metrics_query()
        log.debug(metrics_to_query)
        for metric, fields in metrics_to_query.iteritems():
            # TODO: How big set of data we should analyze?
            # In the meaning of time (where statement)
            where_clause = "time > now() - " + TIME_FROM_NOW +\
            " and source = \'%s\'" % self.source
            query_to_execute = compile(
                Q().tables('"' + metric + '"').
                    fields(fields["field"]).where(where_clause)
            )

            log.debug(self._format_query_to_string(query_to_execute))
            database_output = self._get_data_from_database(
                self._format_query_to_string(query_to_execute))

            data_to_test[metric] = (database_output, fields)

        self._result[DATA_FIELD] = data_to_test
        return self._result


    def _filter_data_from_serenity(self, serenity_data):
        metric_value = dict()

        conf_file = open(SERENITY_METRICS_CONF_FILE, 'r')
        for record in conf_file:
            if record.startswith('#'):  # it's just a comment, move along
                continue
            if record in serenity_data:
                metric_value[record] = serenity_data[record]

        return metric_value

    def _get_which_metrics_query(self):
        table_field_dict = dict()

        conf_file = open(DATABASE_METRICS_CONF_FILE, 'r')
        for record in conf_file:
            if record.startswith('#'):  # it's just a comment, move along
                continue

            key_value = record.split()
            table_field_dict[key_value[0]] = dict()
            table_field_dict[key_value[0]]["field"] = key_value[1]
            table_field_dict[key_value[0]]["cumulated"] = False
            table_field_dict[key_value[0]]["save"] = False
            table_field_dict[key_value[0]]["mul"] = 1
            table_field_dict[key_value[0]]["post_effect_needed"] = False
            for i in xrange(1, (len(key_value)-1)):
                if key_value[i] == "cumulated":
                    table_field_dict[key_value[0]]["cumulated"] = True
                    table_field_dict[key_value[0]]["post_effect_needed"] = True
                elif key_value[i] == "save":
                    table_field_dict[key_value[0]]["save"] = True
                    table_field_dict[key_value[0]]["post_effect_needed"] = True
                elif "mul" in key_value[i]:
                    mul = key_value[i].split('=')
                    table_field_dict[key_value[0]]["mul"] = float(mul[1])
                    table_field_dict[key_value[0]]["post_effect_needed"] = True

        return table_field_dict

    def _format_query_to_string(self, query):
        return query[0] % tuple(query[1])

    def _get_data_from_database(self, query_to_execute):
        return self._dbConnector.query_data(query_to_execute)
