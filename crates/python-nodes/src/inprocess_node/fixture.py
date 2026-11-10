import numpy as np


class TextNode:
    def initialize(self, config):
        pass

    def process_shared(self, data, ctx):
        return data.upper()

    def finalize(self):
        pass


class SourceNode:
    def initialize(self, config):
        self.mode = config.get('mode', 'source')
        self.produced = 0
        self.closed = False

    def process_shared(self, data, ctx):
        return {'data_type': 'json', 'data': {
            'produced': self.produced, 'closed': self.closed}}

    def tick_shared(self, tick, ctx):
        def produce():
            try:
                if self.mode == 'fail':
                    raise RuntimeError('source-tick-failure')
                if tick['frame_idx'] != 1:
                    return
                for _ in range(3 if self.mode == 'many' else 1):
                    loan = ctx.allocate_audio(4)
                    view = np.asarray(loan)
                    address = view.__array_interface__['data'][0]
                    view[:] = 0.25
                    del view
                    self.produced += 1
                    yield {'data_type': 'audio', 'samples': np.asarray(loan.publish()),
                           'sample_rate': 24000, 'channels': 1,
                           'metadata': {**tick, 'producer_address': address,
                                        'session': ctx.session_id, 'node': ctx.node_id}}
            finally:
                self.closed = True
        self.active = produce()
        return self.active

    def finalize(self):
        pass


class SharedNode:
    def initialize(self, config):
        self.mode = config.get('mode', 'pass')
        self.produced = 0
        self.closed = False
        self.kept = []

    def process_shared(self, data, ctx):
        if data['data_type'] == 'json':
            if data['data'].get('clear'):
                self.kept.clear()
            return {'data_type': 'json', 'data': {
                'produced': self.produced, 'closed': self.closed,
                'session': ctx.session_id, 'node': ctx.node_id,
                'retained': len(self.kept),
            }}
        if self.mode == 'retain':
            self.kept.append(data['samples'][1:])
        if self.mode == 'private':
            return {**data, 'samples': np.array(data['samples'], copy=True)}
        if self.mode == 'gain':
            loan = ctx.allocate_audio(len(data['samples']))
            view = np.asarray(loan)
            address = view.__array_interface__['data'][0]
            np.multiply(data['samples'], np.float32(0.5), out=view)
            del view
            return {**data, 'samples': np.asarray(loan.publish()),
                    'metadata': {'producer_address': address}}
        return data

    def process_streaming_shared(self, data, ctx):
        def produce():
            try:
                for _ in range(3):
                    loan = ctx.allocate_audio(4)
                    view = np.asarray(loan)
                    view[:] = self.produced
                    del view
                    self.produced += 1
                    yield {**data, 'samples': np.asarray(loan.publish())}
            finally:
                self.closed = True
        # Retain the generator to prove the host closes it on callback failure.
        self.active = produce()
        return self.active

    def finalize(self):
        pass
