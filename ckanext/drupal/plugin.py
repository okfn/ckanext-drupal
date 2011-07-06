from ckan.plugins import IConfigurer
from ckan.plugins import implements, SingletonPlugin
from sqlalchemy import types, Column, Table
from sqlalchemy import MetaData, create_engine

class Drupal(SingletonPlugin):
    '''initial test of plugin'''
    implements(IConfigurer)


    def update_config(self, config):
        config['ckan.site_title'] = 'CKAN-Drupal'

        url = config['drupal.db_url'] 

        self.engine = create_engine(url)
        self.metadata = MetaData()

        PACKAGE_NAME_MAX_LENGTH = 100
        PACKAGE_VERSION_MAX_LENGTH = 100

        package_table = Table('ckan_package', self.metadata,
            Column('id', types.UnicodeText, primary_key=True),
            Column('name', types.Unicode(PACKAGE_NAME_MAX_LENGTH),
                   nullable=False, unique=True),
            Column('title', types.UnicodeText),
            Column('version', types.Unicode(PACKAGE_VERSION_MAX_LENGTH)),
            Column('url', types.UnicodeText),
            Column('author', types.UnicodeText),
            Column('author_email', types.UnicodeText),
            Column('maintainer', types.UnicodeText),
            Column('maintainer_email', types.UnicodeText),                      
            Column('notes', types.UnicodeText),
            Column('license_id', types.UnicodeText),
        )
        self.metadata.create_all(self.engine)

