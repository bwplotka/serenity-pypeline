# Simple class for testing workflows

# TODO: probably should be removed?
class Echo(object):

    def __init__(self, *args, **kwargs):
        pass

    def run(self, **kwargs):
        return kwargs
