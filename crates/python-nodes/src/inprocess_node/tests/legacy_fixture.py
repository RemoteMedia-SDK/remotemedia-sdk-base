class LegacyNode:
    def initialize(self, config):
        self.mode = config.get('mode', 'echo')

    def process(self, data):
        if self.mode == 'numpy':
            import numpy as np
            return {**data, 'samples': np.asarray(data['samples'], dtype=np.float32)[::-1]}
        if self.mode == 'error':
            raise ValueError('legacy process sentinel')
        if self.mode == 'bad_samples':
            return {**data, 'samples': [1.0, object(), 3.0]}
        if self.mode == 'changing':
            class Changing(list):
                def __iter__(self):
                    yield 1.0
                    yield 2.0
            return {**data, 'samples': Changing([1.0])}
        return data

    def process_streaming(self, data):
        for index in range(3):
            if self.mode == 'stream_error' and index == 1:
                raise ValueError('legacy stream sentinel')
            yield data

    def finalize(self):
        pass
