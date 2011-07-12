import datetime
from sqlalchemy import types, Column, Table
from sqlalchemy.sql import select, and_
from sqlalchemy import MetaData, create_engine
import json
import time

from ckan.plugins import IConfigurer, ISession
from ckan.plugins import implements, SingletonPlugin
import ckan.model as model


class Drupal(SingletonPlugin):
    '''initial test of plugin'''
    implements(IConfigurer)
    implements(ISession, inherit=True)

    def update_node(self, row, session, conn):

        timestamp=int(time.time())

        result = conn.execute(
            self.node_revisions.insert().values(
                nid=0,
                uid=1,
                title=row.get('title',''),
                body=row.get('notes',''),
                teaser=row.get('title',''),
                log='%s-%s'%(session.revision.id,session.revision.message),
                timestamp=timestamp,
                format=1
            )
        )
        vid = result.inserted_primary_key[0]
        

        if 'nid' in row:
            result = conn.execute(
                self.node.update().where(
                    self.node.c.nid == row['nid']
                ).values(
                    vid=vid,
                    title=row.get('title',''),
                )
            )
            nid = row['nid']
        else:
            result = conn.execute(
                self.node.insert().values(
                    vid=vid,
                    type='package',
                    language='',
                    title=row.get('title',''),
                    uid=1,
                    status=0,
                    created=timestamp,
                    changed=timestamp,
                    comment=2,
                    promote=0,
                    moderate=0,
                    sticky=0,
                    tnid=0,
                    transalate=0,
                )
            )
            nid = result.inserted_primary_key[0]
            row['nid'] = nid

        conn.execute(
            self.node_revisions.update().where(
                self.node_revisions.c.vid == vid).values(
                    nid=nid,
                )
        )
        return nid

    def get_package_row(self, conn, package_id):

        return conn.execute(
            select(
                [self.package_table],
                self.package_table.c.id == package_id
            )
        ).fetchone()

            
    def update_drupal(self, session, conn):

        obj_cache = session._object_cache
        new = obj_cache['new']
        changed = obj_cache['changed']
        deleted = obj_cache['deleted']

        try:
            update_date = session.revision.timestamp
        except AttributeError:
            update_date = datetime.datetime.now()

        package_rows = {}
        package_tags = {}

        inserts = []
        updates = []
        deletes = []

        updated_nids = []

        for obj in new:
            if hasattr(obj, 'state') and 'pending' in obj.state:
                continue
            if hasattr(obj, 'current') and obj.current <> '1':
                continue
            if isinstance(obj, (model.Package, model.PackageRevision)):
                insert = self.add_insert(obj, self.package_table)
                package_rows[insert['id']] = insert
                inserts.append(insert)
            if isinstance(obj, (model.Resource, model.ResourceRevision)):
                inserts.append(self.add_insert(obj, self.resource_table))
            if isinstance(obj, (model.PackageExtra, model.PackageExtraRevision)):
                inserts.append(self.add_insert(obj, self.package_extra_table))
            if isinstance(obj, (model.PackageTag, model.PackageTagRevision)):
                inserts.append(self.add_tag(obj, conn))

        for obj in changed:
            if hasattr(obj, 'state') and 'pending' in obj.state:
                continue
            if hasattr(obj, 'current') and obj.current <> '1':
                continue
            if isinstance(obj, (model.Package, model.PackageRevision)):
                update = self.add_update(obj, self.package_table)
                package_row = package_rows.get(
                    obj.id,
                    self.get_package_row(conn, obj.id)
                )
                
                update['nid'] = package_row['nid']
                package_rows[update['id']] = update
                updates.append(update)
            if isinstance(obj, (model.Resource, model.ResourceRevision)):
                if obj.state == 'deleted':
                    deletes.append(self.add_delete(obj, self.resource_table, conn))
                else:
                    updates.append(self.add_update(obj, self.resource_table))
            if isinstance(obj, (model.PackageExtra, model.PackageExtraRevision)):
                if obj.state == 'deleted':
                    deletes.append(self.add_delete(obj, self.package_extra_table, conn))
                else:
                    updates.append(self.add_update(obj, self.package_extra_table))
            if isinstance(obj, (model.PackageTag, model.PackageTagRevision)):
                if obj.state == 'deleted':
                    deletes.append(self.remove_tag(obj, conn))

        for obj in deleted:
            if hasattr(obj, 'state') and 'pending' in obj.state:
                continue
            if hasattr(obj, 'current') and obj.current <> '1':
                continue
            if isinstance(obj, (model.Package, model.PackageRevision)):
                delete = self.add_delete(obj, self.package_table)
                package_row = self.get_package_row(conn, obj.id)
                delete['nid'] = delete['nid']
                package_rows[delete['id']] = delete
                deletes.append(delete)
            if isinstance(obj, (model.PackageExtra, model.PackageExtraRevision)):
                deletes.append(self.add_delete(obj, self.package_extra_table, conn))
            if isinstance(obj, (model.Resource, model.ResourceRevision)):
                deletes.append(self.add_delete(obj, self.resource_table, conn))
            if isinstance(obj, (model.PackageTag, model.PackageTagRevision)):
                deletes.append(self.remove_tag(obj, conn))

        for row in inserts + updates + deletes:
            if 'package_id' in row:
                package_id = row['package_id']
            else:
                package_id = row['id']
            if package_id in package_rows:
                package_rows[package_id]['update_date'] = update_date
            else:
                package_row = self.get_package_row(conn, package_id)
                update = {'__table': self.package_table, 
                          'id': package_id,
                          'nid': package_row['nid'],
                          'update_date': update_date,
                          'title': package_row['title'],
                          'notes': package_row['notes']}
                updates.append(update)
                package_rows[package_id] = update

        for package_row in package_rows.values():
            nid = self.update_node(package_row, session, conn)
            updated_nids.append(nid)

        for row in inserts + updates + deletes:
            if 'package_id' in row:
                row['nid'] = package_rows[row['package_id']]['nid']

        for row in inserts:
           table = row.pop('__table') 
           conn.execute(table.insert().values(**row))

        for row in updates:
           table = row.pop('__table') 
           id = row.pop('id')
           conn.execute(table.update().where(table.c.id==id).values(**row))

        for row in deletes:
           table = row.pop('__table') 
           if table == self.term_node:
               conn.execute(
                   table.delete().where(
                       and_(table.c.nid==row['nid'],table.c.tid==row['tid'])
                   )
               )
               continue
           id = row.pop('id')
           conn.execute(table.delete().where(table.c.id==id))


    def add_insert(self, obj, table):

        insert = {'__table': table}
        for column in table.c:
            value = getattr(obj, column.name, None)
            if value is not None:
                insert[column.name] = value
        if isinstance(obj, model.Resource):
            insert['package_id'] = obj.resource_group.package_id
            insert['extras'] = json.dumps(insert['extras'])
        if isinstance(obj, model.ResourceRevision):
            insert['package_id'] = obj.coninuity.resource_group.package_id
            insert['extras'] = json.dumps(insert['extras'])
        return insert

    def add_update(self, obj, table):

        update = {'__table': table}
        for column in table.c:
            value = getattr(obj, column.name, None)
            if value is not None:
                update[column.name] = value
        if isinstance(obj, model.Resource):
            update['package_id'] = obj.resource_group.package_id
            update['extras'] = json.dumps(update['extras'])
        if isinstance(obj, model.ResourceRevision):
            update['package_id'] = obj.continuity.resource_group.package_id
            update['extras'] = json.dumps(update['extras'])
        return update

    def add_delete(self, obj, table, conn):

        delete = {'__table': table}
        for column in table.c:
            value = getattr(obj, column.name, None)
            if value is not None:
                delete[column.name] = value
        if isinstance(obj, (model.Resource, model.ResourceRevision)):
            package_id = conn.execute(
                select(
                    [self.resource_table],
                    self.resource_table.c.id == obj.id
                )
            ).fetchone()["package_id"]
            delete["package_id"] = package_id
        return delete

    def add_tag(self, obj, conn):

        term = conn.execute(
            select([self.term_data]).where(
                and_(
                    self.term_data.c.vid == self.vid,
                    self.term_data.c.name == obj.tag.name
                )
            )
        ).fetchone()

        if not term:
            insert_term = conn.execute(
                self.term_data.insert().values(
                    vid=self.vid,
                    name=obj.tag.name
                )
            )
            tid = insert_term.inserted_primary_key[0]
        else:
            tid = term['tid']

        return {
            '__table': self.term_node,
            'package_id': obj.package_id,
            'tid': tid,
            'vid': self.vid
        }

    def remove_tag(self, obj, conn):

        tid = conn.execute(
            select([self.term_data]).where(
                and_(
                    self.term_data.c.vid == self.vid,
                    self.term_data.c.name == obj.tag.name
                )
            )
        ).fetchone()['tid']

        return {
            '__table': self.term_node,
            'package_id': obj.package_id,
            'tid': tid,
            'vid': self.vid
        }


    def before_commit(self, session):

        session.flush()
        if not hasattr(session, '_object_cache'):
            return

        conn = self.engine.connect()
        trans = conn.begin()

        try:
            self.update_drupal(session, conn)
            trans.commit()
        except:
            trans.rollback()
            session.rollback()
            raise
        finally:
            conn.close()

    def update_config(self, config):
        config['ckan.site_title'] = 'CKAN-Drupal'

        url = config['drupal.db_url'] 
        self.vid = int(config.get('drupal.vid', 1))

        self.engine = create_engine(url, echo=True)
        self.metadata = MetaData(self.engine)

        PACKAGE_NAME_MAX_LENGTH = 100
        PACKAGE_VERSION_MAX_LENGTH = 100

        self.package_table = Table('ckan_package', self.metadata,
            Column('nid', types.Integer, unique=True),
            Column('id', types.Unicode(100), primary_key=True),
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
            Column('update_date', types.DateTime),
            Column('state', types.UnicodeText),
            Column('completed', types.Boolean),
        )

        self.resource_table = Table(
            'ckan_resource', self.metadata,
            Column('nid', types.Integer),
            ## cache of package id to make things easier
            Column('package_id', types.UnicodeText),
            ##
            Column('id', types.Unicode(100), primary_key=True),
            Column('resource_group_id', types.UnicodeText),
            Column('package_id', types.UnicodeText),
            Column('url', types.UnicodeText, nullable=False),
            Column('format', types.UnicodeText),
            Column('description', types.UnicodeText),
            Column('hash', types.UnicodeText),
            Column('position', types.Integer),
            Column('extras', types.UnicodeText),
            )

        self.package_extra_table = Table('ckan_package_extra', self.metadata,
            Column('nid', types.Integer),
            Column('id', types.Unicode(100), primary_key=True),
            Column('package_id', types.UnicodeText),
            Column('key', types.UnicodeText),
            Column('value', types.UnicodeText),
        )

        self.node = Table('node', self.metadata,
                          autoload = True)
        self.node_revisions = Table('node_revisions', self.metadata,
                                   autoload = True)
        self.term_node = Table('term_node', self.metadata,
                                   autoload = True)
        self.term_data = Table('term_data', self.metadata,
                                   autoload = True)

        self.metadata.create_all(self.engine)

