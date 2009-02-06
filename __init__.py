import pdb
from decimal import Decimal
from time import sleep

from django.utils.datastructures import SortedDict
from django.utils.encoding import smart_unicode
from django.db import IntegrityError

from django.db.models.fields import BooleanField 
from django.db.models.fields import NullBooleanField 
from django.db.models.fields import IntegerField 
from django.db.models.fields import DecimalField 
from django.db.models.fields import DateTimeField 
from django.db.models.fields import DateField 
from django.db.models.fields import CharField 
from django.db.models.fields import TextField 
from django.db.models import ForeignKey 
from django.db.models import OneToOneField 
from django.db.models import ManyToManyField 
from django.db.models.query import QuerySet

class ValidationError(Exception):
    "(Almost straight copy from django.forms)"
    def __init__(self, message):
        " ValidationError can be passed any object that can be printed (usually a string) or a list of objects. "
        if isinstance(message, list):
            self.messages = ([smart_unicode(msg) for msg in message])
        else:
            message = smart_unicode(message)
            self.messages = ([message])

    def __str__(self):
        # This is needed because, without a __str__(), printing an exception
        # instance would result in this:
        # AttributeError: ValidationError instance has no attribute 'args'
        # See http://www.python.org/doc/current/tut/node10.html#handling
        return repr(self.messages)

class Field(object):
	" Derived from django.forms.Field "
	creation_counter = 0
	def __init__(self,slave_field_name,uses_import=None,max_length=None,max_value=None,if_null=None, value=None,verify_by=None,map=None,clean_function=None):
		if clean_function:
			self.clean_function=clean_function
		self.map = map
		self.test = False 
		self.max_length = max_length
		self.verify_by = verify_by or []
		self.value = value
		self.slave_field_name = slave_field_name
		self.max_value = max_value
		self.if_null = if_null
		self.variants = None
		self.importation = uses_import 

		self.creation_counter = Field.creation_counter
		Field.creation_counter += 1
	
	def prep_work(self,master_field_name,parent):
		" This is basically a second __init__ "
		if not hasattr(self,'parent'):
			self.master_field_name = master_field_name
			self.parent = parent
			self.master = self.parent.Meta.master._meta.get_field(master_field_name)
			self.slave = None
			if self.slave_field_name:
				self.slave = self.parent.Meta.slave._meta.get_field(self.slave_field_name)

			if isinstance(self.master,ForeignKey) or isinstance(self.master,OneToOneField):
				if self.importation:
					unique = False
					self.importation = self.importation()
					
					if isinstance(self.master,OneToOneField):
						unique = True

					" Recursion ahoy!! "

					self.importation.do_import(unique=unique,verify_by=self.verify_by) 

					" This next line is an important linkage to make sure we don't collect variants twice "

					self.variants = self.importation.dataset
				else:
					prefilled_variants = {} 
					#if self.master_field_name in self.parent.field_variants.keys():
						#print "%s using variants from %s, finding %s" % (self.master_field_name, self.parent, len(self.parent.field_variants))
						#prefilled_variants = self.parent.field_variants[self.master_field_name]

					self.variants = self._collect_variants(prefilled_variants)
					#self.parent.field_variants[self.master_field_name] = self.variants

	def _collect_variants(self,prefilled_variants):
		variants = {}
		#variants.update(prefilled_variants)
		#print "Prefilled: %s" % prefilled_variants
		#This runs through the values for all of its possible slave
		#values, prompting you to associate each as 'variants' to the master field.

		#When this is done the conversion will loop through uninterrupted

		# Use whole queryset here because clean_FOO override might need other fields in old QS.
		slave_iterable = self.parent.Meta.queryset.order_by(self.slave_field_name) 
		if not hasattr(self.parent, 'clean_%s' % self.master_field_name):  
			" clean_FOO attributes need the whole record, not just one field's value. "
			slave_iterable = slave_iterable.values_list(self.slave_field_name,flat=True).distinct()	

		print "Gathering variants for %s (%s total) | %s" % (self.master_field_name,len(slave_iterable),self.parent)

		associations = list(self.master.rel.to.objects.all())
		self._show_assoc_options(associations)

		for v in slave_iterable: # Can be QS or values_list depending on above.
			if hasattr(self.parent,'clean_%s' % self.master_field_name):
				v = getattr(self.parent,'clean_%s' % self.master_field_name)(v,None)
			v = str(v).lower()
			if not v in variants:
				variants[v], associations = self._record_associate(v,associations)
		return variants

	def _show_assoc_options(self,associations):
		if not self.test:
			count = 1
			for i in associations:
				print "[%s] %s" % (count, i)
				count += 1

	def _record_associate(self,slave_value,associations):
		if not self.master.rel.to.objects.all().count():
			return self._record_create(slave_value), associations # There's nothing to pick from. Create one!

		while True:
			if self.test and associations:
				return associations[0], assocations
			else:	# or let the user pick from the PK list already given.
				input = raw_input("Which above %s to associate with value '%s'? 'LIST' | 'NEW' | <Enter> for None:\t" % (self.master_field_name,slave_value))

			if input == '':	
				return None, associations
			elif input == 'LIST':
				self._show_assoc_options(associations)
				continue
			elif input == 'NEW':
				new_record = self._record_create(slave_value)
				associations = list(self.master.rel.to.objects.all())
				return new_record, associations
			try:
				input = int(input) - 1
				if input in range(0,len(associations)):
					return associations[input], associations
			except ValueError:
				print "You fail. Retry."
	
	def _record_create(self,value):
		print "Creating new [%s] for old value:\t%s" % (self.master_field_name,value) 
		new_model = {}
		if value == None:
			return None
		while True: #Loop through and have the user assign field values
			for field in self.master.rel.to._meta.fields:
				if not field.name in ['pk','id']:
					while True:
						try: #In case their input is no good.
							input = raw_input("Value for field '%s'? (value or 'SKIP','RETRY'):\t" % field.name)
							if input == 'SKIP':
								return None
							elif input == 'RETRY':
								return self._record_create(value)
							new_model[field.name] = input 
							break
						except ValueError:
							print "You fail. Bad. Please try again"
			try:
				new_object = self.master.rel.to.objects.get_or_create(**new_model)[0]
				print "New object created:\t%s" % new_object 
				return new_object
			except ValueError:
				print "Object failed to create. Please fix these keys and continue:\t%s" % new_model
				pdb.set_trace()
				return self.master.rel.to.objects.get_or_create(**new_model)[0]

	def clean(self,value):
		if self.value != None: 
			" Maybe user provided a value=Value on the field "
			return self.value
	
		if self.map:
			if value in self.map.keys():
				value = self.map[value]

		if hasattr(self,'clean_function'):
			value = self.clean_function(value)

		if self.max_length and value == str(value):
			value = value[:self.max_length]

		if isinstance(self.master,ForeignKey) or isinstance(self.master,OneToOneField):
			if str(value).lower() in self.variants.keys(): 
				return self.variants[str(value).lower()]
			return None	# Return nothing. Maybe clean_FOO will be populating this value.
				
		elif isinstance(self.master,DecimalField):
			if not value and not self.master.null:
				return self.if_null

			if value != None:
				value = Decimal('%s' % value)
				if self.max_value and self.max_value < value:
					return self.max_value
			return value

		elif isinstance(self.master,CharField) or isinstance(self.master,TextField):
			if value:
				value = '%s' % value.encode('ascii','ignore')
				if getattr(self.master,'max_length',False) and len(value) > self.master.max_length:
					return '%s' % value[:self.master.max_length]
			else:
				if not self.if_null == None:
					return '%s' % self.if_null
			return value

		# Catchall
		if not value:
			return self.if_null
		if getattr(self.master,'max_length',False) and len(value) > self.master.max_length:
			return '%s' % value[:self.master.max_length]
		return value

def get_declared_fields(bases, attrs, with_base_fields=True):
    """
    Create a list of form field instances from the passed in 'attrs', plus any
    similar fields on the base classes (in 'bases'). This is used by both the
    Form and ModelForm metclasses.

    If 'with_base_fields' is True, all fields from the bases are used.
    Otherwise, only fields in the 'declared_fields' attribute on the bases are
    used. The distinction is useful in ModelForm subclassing.
    Also integrates any additional media definitions
    """
    fields = []
    for field_name, obj in attrs.items():
	    if isinstance(obj,Field):
		    attribute = attrs.pop(field_name)
		    fields.append((field_name,attribute))

    fields.sort(lambda x, y: cmp(x[1].creation_counter, y[1].creation_counter))

    # If this class is subclassing another Form, add that Form's fields.
    # Note that we loop over the bases in *reverse*. This is necessary in
    # order to preserve the correct order of fields.
    for base in bases[::-1]:
        if hasattr(base, 'fields'):
            fields = base.fields.items() + fields

    return SortedDict(fields)

class DeclarativeFieldsMetaclass(type):
    """
    Metaclass that converts Field attributes to a dictionary called
    'fields', taking into account parent class 'fields' as well.
    """
    def __new__(cls, name, bases, attrs):
        attrs['fields'] = get_declared_fields(bases, attrs)
        new_class = super(DeclarativeFieldsMetaclass,cls).__new__(cls, name, bases, attrs)
        return new_class

class BaseImport(object):
	def __init__(self,override_values=None, queryset=None, verbose=False,slave=False,verify_by=None):
		print "init %s" % self
		self.field_variants = {} 
		if slave:
			self.Meta.slave = slave

		self.dataset = {}	# Keeps a dict of items created by. {'slave_key' : master_object }
		self.verify_by = verify_by or []
		
		self.verbose = verbose
		self.Meta.queryset = queryset
		self.override_values = override_values or {}
		self.totals = {'created' : 0, 'processed': 0 }	# General numbers on the work done in this instance. 

		" Now let's make sure all req'd fields have been specified "
		for i in self.Meta.master._meta.local_fields:
			if not i.name == "id" and not i.name in self.get_all_master_field_names() and not self.Meta.master._meta.get_field(i.name).null:
				print "Warning! Field '%s' in model %s is not-nullable! You didn't specify it in fields! Better take care of it in clean()!" % (i.name,self.Meta.master._meta.db_table)

		if isinstance(self.Meta.slave,QuerySet):
			if not getattr(self.Meta,'queryset',None):
				self.Meta.queryset = self.Meta.slave
			self.Meta.slave = self.Meta.queryset.model
		else:
			if not getattr(self.Meta,'queryset',None):
				self.Meta.queryset = self.Meta.slave.objects.all()
		
		for i in self.fields:
			self.fields[i].prep_work(i,self)

	def get_all_master_field_names(self):
		master_names = []
		for i in self.fields:
			master_names.append(i)
		return master_names

	def do_import(self,verify_by=None,unique=False):
		print "Importation: %s" % self
		for slave_record in self.Meta.queryset:
			new_model = None
			self.totals['processed'] += 1
			if verify_by:
				new_model = self._soft_import(slave_record,verify_by)

			if not new_model:
				new_model = self._hard_import(slave_record,unique)

			self.dataset[str(slave_record.pk)] = new_model

			if hasattr(self,'post_save'):
				self.post_save(slave_record,new_model)

		print "%s" % self.totals

	def _soft_import(self,slave_record,verify_by):
		"Used to match a table only by one field"

		"Keep in mind this *NEVER* works the first time through the slave table. Why? Can't soft-match on nothing!"

		m_field = verify_by[0]

		if len(verify_by) == 1:
			s_field = verify_by[0]
		else:
			s_field = verify_by[1]
		
		ver_filter = {}
		ver_filter[m_field] = getattr(slave_record,s_field)

		try:
			return self.Meta.master.objects.get(**ver_filter)
		except self.Meta.master.DoesNotExist:
			return None

	def _hard_import(self,slave_record,unique):
		"Regular import. Matches two tables by all fields."
		cleaned_data = {}
		for k, field in self.fields.iteritems():
			# ManyToMany isn't going to be done at this point.
			#if isinstance(field,ManyToManyField):
			#	continue

			value = None
			if field.slave_field_name: # There may be no slave field (didn't exist in old table) 
				value = getattr(slave_record,field.slave_field_name)

			if value in self.override_values.keys():
				cleaned_data[k] = self.override_values[k]
			else:
				cleaned_data[k] = self.fields[k].clean(value)

			if hasattr(self, 'clean_%s' % k): # use custom clean_FOO override
				cleaned_data[k] = getattr(self,'clean_%s' % k)(slave_record,cleaned_data)

		if hasattr(self,'clean'):
			cleaned_data = self.clean(slave_record,cleaned_data)

		if not cleaned_data: # There was nothing to create!
			return None

		try:
			new_model, created = self.Meta.master.objects.get_or_create(**cleaned_data)
			if created:
				self.totals['created'] += 1
			return new_model
		except self.Meta.master.MultipleObjectsReturned:
			"""
			We've already gone through the importation once and want to avoid re-creating records.
			However, two imported models turned out to be exactly the same. Because A OneToOneField may have
			been involved, we must somehow respect the existance of two identical models and reliably match
			them up after all is said and done.

			For now we use the first of a QuerySet of those models, excluding those that have already been used!

			Potential bug here is if many different models OneToOne with this model. In this case, its liable to
			pick up one of the other model's OneToOne relationships.
			"""
			if not unique:
				raise self.Meta.master.MultipleObjectsReturned
			pks_so_far = []
			for i in self.dataset:
				pks_so_far.append(self.dataset[i].pk)
			return self.Meta.master.objects.exclude(pk__in=pks_so_far)[0]
		except:
			print '==ERR0R=ADDING=MODEL=======(Probably your fault)==========='
			print '%s' % cleaned_data
			print '===========You\'re in shell! Find out why!================='
			pdb.set_trace()
	
class Import(BaseImport):
	__metaclass__ = DeclarativeFieldsMetaclass
