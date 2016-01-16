from abc import abstractmethod, ABCMeta

__author__ = 'MacRomanowski'


class FilterStep(object):
    __metaclass__ = ABCMeta

    def __init__(self, conf):
        pass

    @abstractmethod
    def run(self, **kwargs):
        pass