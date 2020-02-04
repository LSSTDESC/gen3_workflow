class gen3_workflow(object):
    '''
    Example class for gen3_workflow package.
    '''
    def __init__(self, message):
        self.message = message

    def run(self, raise_error=False):
        if raise_error:
            raise RuntimeError()
        return self.message
