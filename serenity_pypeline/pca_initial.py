from filter_step import FilterStep

__author__ = 'MacRomanowski'


class PcaInitial(FilterStep):

    def __init__(self, conf):
        super(FilterStep, self).__init__()

    def run(self, **kwargs):
        print 'Executing: PcaInitial'