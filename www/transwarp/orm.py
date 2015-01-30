#/usr/bin/env python
#encoding:utf-8

import logging, time

import db

class Field(object):
	'''
	@summary: 定义数据库中表的字段属性（字段名、字段类型、是否主键、缺省值、是否为Null等）。
	'''

	_count = 0

	def __init__(self, **kwargs):
		'''
		@summary: 初始化每个数据库表字段的属性，包括：名称、类型、是否主键等。
		@param: **kwargs: 字段属性字典
		'''
		self.name = kwargs.get('name', None)					# 设置“name”属性（字段名称）的类型，缺省设置为None;
		self._default = kwargs.get('default', None)				# 设置“default”属性（字段缺省值？）的类型；
		self.primary_key = kwargs.get('primary_key', False)		# 设置“primary_key”属性（是否为主键）的类型；
		self.nullable = kwargs.get('nullable', False)			# 设置“nullable”属性（是否可以为Null）的类型，表示能否设置某个字段为null；
		self.updatable = kwargs.get('updatable', True)			# 设置“updatable”属性（是否可以update）的类型；
		self.insertable = kwargs.get('insertable', True)		# 设置“insertable”属性的类型；
		self.ddl = kwargs.get('ddl', '')						# 作用是什么？（貌似是字段的描述内容）
		self._order = Field._count								# 统计Filed类被实例化的次数；
		Field._count = Field._count + 1							# 作用是什么？统计实例化次数。

	@property
	def default(self):
		'''
		'''
		d = self._default
		return d() if callable(d) else d	# 如果d是可调用的，返回d()，否则返回d；

	def __str__(self):
		'''
		@summary：重写__str__方法，打印Filed实例时显示为'<类名：字段名>'
		'''
		s = ['<%s: %s, %s, default(%s), ' % (self.__class__.__name__, self.name, self.ddl, self._default)]
		self.nullable and s.append('N')
		self.updatable and s.append('U')
		self.insertable and s.append('I')
		s.append('>')
		return ''.join(s)

class StringField(Field):
	'''
	@summary: 定义字符串类型的字段
	'''
	def __init__(self, **kwargs):
		'''
		@summary: 初始化函数
		@param: **kwargs：{字段属性1:类型, 字段属性2:类型, ......}，如{'name':'varchar(255)', ......}
		'''
		if not 'default' in kwargs:
			kwargs['default'] = ''
		if not 'ddl' in kwargs:
			kwargs['ddl'] = 'varchar(255)'
		super(self.__class__, self).__init__(**kwargs)

class IntegerField(Field):
	'''
	@summary: 定义整数类型的字段
	'''
	def __init__(self, **kwargs):
		'''
		'''
		# Integer字段缺省值为0
		if not 'default' in kwargs:
			kwargs['default'] = 0
		if not 'ddl' in kwargs:
			kwargs['ddl'] = 'bigint'
		super(self.__class__, self).__init__(**kwargs)

class FloatField(Field):
	'''
	@summary: 定义浮点型类型的字段
	'''
	def __init__(self, **kwargs):
		'''
		'''
		if not 'default' in kwargs:
			kwargs['default'] = 0.0
		if not 'ddl' in kwargs:
			kwargs['ddl'] = 'real'
		super(self.__class__, self).__init__(**kwargs)

class BooleanField(Field):
	'''
	@summary: 定义布尔类型的字段
	'''
	def __init__(self, **kwargs):
		'''
		'''
		if not 'default' in kwargs:
			kwargs['default'] = False
		if not 'ddl' in kwargs:
			kwargs['ddl'] = 'bool'
		super(self.__class__, self).__init__(**kwargs)

class TextField(Field):
	'''
	@summary: 定义文本类型的字段
	'''
	def __init__(self, **kwargs):
		if not 'default' in kwargs:
			kwargs['default'] = ''
		if not 'ddl' in kwargs:
			kwargs['ddl'] = 'text'
		super(self.__class__, self).__init__(**kwargs)

class BlobField(Field):
	'''
	@summary: 定义Blob类型的字段（二进制大对象）
	'''
	def __init__(self, **kwargs):
		if not 'default' in kwargs:
			kwargs['default'] = ''
		if not 'ddl' in kwargs:
			kwargs['ddl'] = 'blob'
		super(self.__class__, self).__init__(**kwargs)

class VersionField(Field):
	'''
	@summary: 定义Version字段
	'''
	def __init__(self, name=None):
		super(VersionField, self).__init__(name=name, default=0, ddl='bigint')

_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])

def _gen_sql(table_name, mappings):
	'''
	@summary: 生成创建table的sql语句
	@param: table_name: 表名（对应Module名称）
	@param: mappings: 字典，{字段名：字段属性集}，字段属性集应该就是Filed的子类；
	@return: sql语句
	'''
	pk = None
	sql = ['--generating SQL for %s:' % table_name, 'create table `%s` (' % table_name]
	# 对mappings.values()进行排序，根据比较filed的实例化次数大小排序；
	for f in sorted(mappings.values(), lambda x, y: cmp(x._order, y._order)):
		if not hasattr(f, 'ddl'):
			raise StandardError('no ddl in field "%s".' % f)
		ddl = f.ddl
		nullable = f.nullable
		if f.primary_key:
			pk = f.name
		sql.append(nullable and ' `%s` %s,' % (f.name, ddl) or ' `%s` %s not null, ' % (f.name, ddl))
	sql.append(' primary key(`%s`)' % pk)
	sql.append(');')
	return '\n'.join(sql)

class ModelMetaClass(type):
	'''
	@summary: 用于创建Model类的元类，继承自type；具体的实体对象类也通过它来创建，如User。
	'''
	def __new__(cls, name, bases, attrs):
		'''
		@summary: 构造函数，通过该方法定义一个类（主要用于定义具体的Module类，如User等实体类。）；
		@params: cls: 类指针
		@params: name: 类名
		@params: bases：父类元组
		@params：attr：类属性字典
		'''
		# 忽略所有模型类（数据库表）的父类（Model）
		if name == 'Model':
			return type.__new__(cls, name, bases, attrs)

		# 存储全部Model子类的信息（对应具体的表）
		if not hasattr(cls, 'subClasses'):
			cls.subClasses = {}
		if not name in cls.subClasses:
			cls.subClasses[name] = name
		else:
			logging.warning('Redefine class: %s' % name)

		logging.info('Scan ORMapping %s ...' % name)
		mappings = dict()	# 读取cls的Filed字段
		primary_key = None 	# 查找primary_key字段
		# 遍历创建Model子类的属性（k-v）
		for k, v in attrs.iteritems():
			if isinstance(v, Field):
				if not v.name:
					v.name = k
				logging.info('Found mapping: %s => %s' % (k, v))
				# 检查重复的primary key
				if v.primary_key:
					if primary_key:
						raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
					if v.updatable:
						logging.warning('NOTE: Change primary key to non-updatable.')
						v.updatable = False
					if v.nullable:
						logging.warning('NOTE: Change primary key to non-nullable.')
						v.nullable = False
					primary_key = v
				mappings[k] = v
		# 检查primary key是否存在
		if not primary_key:
			raise TypeError('Primary key not defined in class: %s' % name)
		for k in mappings.iterkeys():
			attrs.pop(k)
		# 定义表名（类名小写）
		if not '__table__' in attrs:
			attrs['__table__'] = name.lower()
		attrs['__mappings__'] = mappings
		attrs['__primary_key__'] = primary_key
		attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)	# 生成创建表的sql语句并保存在attrs['__sql__']中
		for trigger in _triggers:
			if not trigger in attrs:
				attrs[trigger] = None
		return type.__new__(cls, name, bases, attrs)

class Model(dict):
	'''
	@summary: Base class for ORM（所有实体对象的父类，从dict类继承，通过ModelMetaClass元类创建。）
	 >>> class User(Model):
		... id = IntegerField(primary_key=True)
		... name = StringField()
		... email = StringField(updatable=False)
		... passwd = StringField(default=lambda: '******')
		... last_modified = FloatField()
		... def pre_insert(self):
		... self.last_modified = time.time()
		>>> u = User(id=10190, name='Michael', email='orm@db.org')
		>>> r = u.insert()
		>>> u.email
		'orm@db.org'
	print User().__sql__()
	-- generating SQL for user:
	create table `user` (
		`id` bigint not null,
		`name` varchar(255) not null,
		`email` varchar(255) not null,
		`passwd` varchar(255) not null,
		`last_modified` real not null,
		primary key(`id`)
	);
	'''
	__metaclass__ = ModelMetaClass

	def __init__(self, **kwargs):
		super(Model, self).__init__(**kwargs)
		# db.create_engine('root', 'qwer1234', 'test', host='10.1.40.5')

	def __getattr__(self, key):
		'''
		@summary: 通过__getattr__方法，实现以model.key方式替换model[key]方式访问字典元素；
		@param: key: 字典的key；
		@return: 对应的值。
		'''
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

	def __setattr__(self, key , value):
		'''
		@summary: 通过__setattr__方法，实现以model.key=value的方式为字典赋值；
		'''
		self[key] = value

	@classmethod
	def get(cls, pk):
		'''
		@summary: 通过primary_key从表中查询
		'''
		d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
		# 将查询结果转换成一个Model子类，并返回。
		return cls(**d) if d else None

	@classmethod
	def find_first(cls, where, *args):
		'''
		@summary: 通过where子句查询
		@param:
		@return: （1）只有1条结果时，返回第1条结果；
				 （2）如果查询结果包含多条记录，只返回第1条；
				 （3）如果查询结果为空，返回None。
		'''
		d = db.select_one('select * from %s %s' % (cls.__table__, where), *args)
		return cls(**d) if d else None

	@classmethod
	def find_all(cls, *args):
		'''
		@summary: 进行全表查询，返回全部查询结果（list）。
		'''
		L = db.select('select * from %s' % cls.__table__)
		return [cls(**d) for d in L]

	@classmethod
	def find_by(cls, where, *args):
		'''
		@summary: 通过where子句查询；
		@param:
		@return: 返回全部查询结果（list）
		'''
		L = db.select('select * from `%s` %s' % (cls.__table__, where), *args)
		return [cls(**d) for d in L]

	@classmethod
	def count_all(cls):
		'''
		@summary: 通过'select count(pk) from table'查询，并返回一个整数。
		'''
		return db.select_int('select count(`%s`) from `%s`' % (cls.__primary_key__.name, cls.__table__))

	@classmethod
	def count_by(cls, where, *args):
		'''
		@summary: 通过'select count(pk) from table where ...'查询，并返回一个整数。
		'''
		return db.select_int('select count(`%s`) from `%s` %s' % (cls.__primary_key__.name, cls.__table__, where), *args)

	def update(self):
		'''
		'''
		self.pre_update and self.pre_update()
		L = []
		args = []
		for k, v in self.__mappings__.iteritems():
			if v.updatable:
				if hasattr(self, k):
					arg = getattr(self, k)
				else:
					arg = v.default
					setattr(self, k, arg)
				L.append('`%s`=?' % k)
				args.append(arg)
		pk = self.__primary_key__.name
		args.append(getattr(self, pk))
		db.update('update `%s` set %s where %s=?' % (self.__table__, ','.join(L), pk), *args)
		return self

	def delete(self):
		self.pre_delete and self.pre_delete()
		pk = self.__primary_key__.name
		args = (getattr(self, pk), )
		db.update('delete from `%s` where `%s`=?' % (self.__table__, pk), *args)
		return self

	def insert(self):
		self.pre_insert and self.pre_insert()
		params = {}
		for k, v in self.__mappings__.iteritems():
			if v.insertable:
				if not hasattr(self, k):
					setattr(self, k, v.default)
				params[v.name] = getattr(self, k)
		db.insert('%s' % self.__table__, **params)
		return self

if __name__=="__main__":

	class User(Model):
		id = IntegerField(primary_key=True)
		name = StringField()
		email = StringField(updatable=False)
		passwd = StringField(default=lambda: '******')
		last_modified = FloatField()
		def pre_insert(self):
			self.last_modified = time.time()
	db.create_engine('root', 'qwer1234', 'test', host='10.1.40.5')
	u = User(id=10190, name='Andy', email='liufei83@163.com')
	r = u.insert()
	print r.email