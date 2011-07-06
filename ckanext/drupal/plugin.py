from ckan.plugins import IConfigurer
from ckan.plugins import implements, SingletonPlugin

class Drupal(SingletonPlugin):
    '''initial test of plugin'''
    implements(IConfigurer)

    def update_config(self, config):
        config['ckan.site_title'] = 'CKAN-drupal'
