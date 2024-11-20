'''
Created on Jan 2, 2020

@author: bstaley
'''
import unittest
import datetime
from jinja2 import Environment, PackageLoader

env = Environment(loader=PackageLoader('maven_status', 'templates'))


class TestTemplates(unittest.TestCase):
    
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testSmoke(self):
        
        def _generate_test_data(component_ids=None,
                                eventids_per_component=None,
                                num_events=None
                                ):
            component_ids = component_ids or ['test_component']
            eventids_per_component = eventids_per_component or range(1)
            num_events = num_events or 1
            
            class TestObject(object):
                pass
            
            res = dict()
            
            for _c in component_ids:
                for _e in eventids_per_component:
                    for _i in range(num_events):
                        _n = TestObject()
                        _n.timetag = datetime.datetime.utcnow()
                        _n.summary = f'{_c}-{_e}-{_i} summary'
                        _n.description = f'{_c}-{_e}-{_i} summary'
                        res.setdefault(_c, dict()).setdefault(_e, []).append(_n)
            return res
        
        test_components = _generate_test_data(component_ids=[f'test-component{_i}' for _i in range(2)],)
        
        template = env.get_template('FailureSummary.html')
        body = template.render(affected_components=test_components)
        self.assertIsNotNone(body)
        
        template = env.get_template('PDSNotification.html')
        body = template.render(affected_components=test_components)
        self.assertIsNotNone(body)
        
        template = env.get_template('ProgressNotification.html')
        body = template.render(affected_components=test_components)
        self.assertIsNotNone(body)
        
        template = env.get_template('StartNotification.html')
        body = template.render(affected_components=test_components)
        self.assertIsNotNone(body)
        
        template = env.get_template('StatusNotification.html')
        body = template.render(affected_components=test_components)
        self.assertIsNotNone(body)
        
