import flask.ext.sqlalchemy as flask_sqlalchemy
from eve.utils import config
from .utils import dict_update


__all__ = ['cval', 'registerSchema']


def cval(**kwargs):
    r'''Add to db.Column info: db.Column(db.String, info=cval(minlength=8))
    '''
    return dict(cval=kwargs)


sqla_type_mapping = {flask_sqlalchemy.sqlalchemy.types.Integer: 'integer',
                     flask_sqlalchemy.sqlalchemy.types.Date: 'date',
                     flask_sqlalchemy.sqlalchemy.types.DateTime: 'datetime',
                     flask_sqlalchemy.sqlalchemy.types.Time: 'time',
                     flask_sqlalchemy.sqlalchemy.types.Boolean: 'boolean',
                     flask_sqlalchemy.sqlalchemy.types.Float: 'float',
                     flask_sqlalchemy.sqlalchemy.types.Numeric: 'decimal',
                     flask_sqlalchemy.sqlalchemy.types.String: 'string',
                    }
                    # TODO: Add the remaining sensible SQL types


def lookup_column_type(intype):
    for sqla_type, api_type in sqla_type_mapping.items():
        if isinstance(intype, sqla_type):
            return api_type
    raise KeyError("{} not a known SQL type".format(intype))


class registerSchema(object):
    """
    Class decorator that scans a Flask-SQLAlchemy db.Model class, prepare an eve schema
    and attach it to the class attributes.
    """

    def __init__(self, resource=None, **kwargs):
        self.resource = resource

    def __call__(self, cls_):
        if hasattr(cls_, '_eve_domain_resource'):
            return cls_

        resource = self.resource or cls_.__name__.lower()

        fields = [config.LAST_UPDATED, config.DATE_CREATED]
        domain = {
            resource: {
                'schema': {},
                'schema_class': cls_.__name__,
                'item_lookup': True,
                'item_lookup_field': '_id',  # TODO: Make these respect the ID_FIELD config of Eve
                'item_url': 'regex("[0-9]+")',
            },
        }

        if hasattr(cls_, '_eve_resource'):
            dict_update(domain[resource], cls_._eve_resource)

        for prop in cls_.__mapper__.iterate_properties:
            if prop.key in (config.LAST_UPDATED, config.DATE_CREATED):
                continue
            schema = domain[resource]['schema'][prop.key] = {}
            self.register_column(prop, schema, fields)

        cls_._eve_domain_resource = domain
        cls_._eve_fields = fields
        return cls_

    @staticmethod
    def register_column(prop, schema, fields):
        if len(prop.columns) > 1:
            raise NotImplementedError  # TODO: Composite column property
        elif len(prop.columns) == 1:
            col = prop.columns[0]
            fields.append(prop.key)
            if isinstance(col, flask_sqlalchemy.sqlalchemy.schema.Column):
                schema['type'] = lookup_column_type(col.type)
                schema['unique'] = col.primary_key or col.unique or False
                schema['required'] = not col.nullable if not col.primary_key else False
                if hasattr(col.type, 'length'):
                    if col.type.length is not None:
                        schema['maxlength'] = col.type.length
                if col.default is not None:
                    schema['default'] = col.default.arg
                    col.default = None
            elif isinstance(col, flask_sqlalchemy.sqlalchemy.sql.expression.ColumnElement):
                schema['type'] = lookup_column_type(col.type)
            else:
                schema['type'] = 'string'
            if col.foreign_keys:
                # Unfortunately SQLAlchemy foreign_keys for a column is a set which does not offer indexing
                # Hence we have to first pop the element, get what we want from it and put it back at the end
                foreign_key = col.foreign_keys.pop()
                relation_resource = foreign_key.target_fullname.split('.')[0]
                schema['data_relation'] = {'resource': relation_resource}
                col.foreign_keys.add(foreign_key)
            if hasattr(col, 'info'):
                cval = col.info.get('cval', {})
                schema.update(cval)
