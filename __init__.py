import pdb
import re
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
			self.custom_clean_function=clean_function

		self.map = map
		self.test = False 
		self.max_length = max_length
		self.verify_by = verify_by or []
		self.value = value
		self.slave_field_name = slave_field_name
		self.max_value = max_value
		self.if_null = if_null
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
					self.importation = self.importation(parent=self)
					
					if isinstance(self.master,OneToOneField):
						unique = True

					" Recursion ahoy!! "
					self.importation.do_import(unique=unique,verify_by=self.verify_by) 
					self._global_variants(self.master.rel.to).update(self.importation.dataset)
				else:

					self._update_global_variants()

	def _alpha_importation(self): # returns top importation fired
		current_level = self 
		while True:
			if hasattr(current_level,'parent') and current_level.parent:
				current_level = current_level.parent
			else:
				if not hasattr(current_level,'master_variants'):
					current_level.master_variants = {}

				return current_level
	
	def _global_variants(self,model_class):
		alpha = self._alpha_importation()
		# Go to first importation and look for master variants of a particular class
		if not model_class.__name__ in alpha.master_variants:
			alpha.master_variants[model_class.__name__] = {}

		return alpha.master_variants[model_class.__name__]

	def _update_global_variants(self):
		variants = {}
		variants.update(self._global_variants(self.master.rel.to))
		#This runs through the values for all of its possible slave
		#values, prompting you to associate each as 'variants' to the master field.
		#When this is done the conversion will loop through uninterrupted

		# Check the initial import for a master variant dict
		# Use whole queryset here because clean_FOO override might need other fields in old QS.
		slave_iterable = self.parent.Meta.queryset.order_by(self.slave_field_name) 
		if not hasattr(self.parent, 'clean_%s' % self.master_field_name):  
			"Since clean_FOO may the whole record, not just one field's value.. get it"
			slave_iterable = slave_iterable.values_list(self.slave_field_name,flat=True).distinct()	

		if self.map:
			# If a map returns an appropriate object, use it!
			for v in slave_iterable:
				if v and not str(v).lower() in variants and \
						isinstance(self.map[v],self.master.__class__):
					variants[str(v).lower()] = self.map[v]

		if hasattr(self,'custom_clean_function'):
			# If a custom clean function returns an appropriate object
			# then go ahead and use that.
			for v in slave_iterable:
				if v and not str(v).lower() in variants and \
						isinstance(self.custom_clean_function(v),self.master.rel.to):
					variants[str(v).lower()] = self.custom_clean_function(v)

		print "Gathering variants for %s (%s total) | %s" % \
				(self.master_field_name,len(slave_iterable),self.parent)

		associations = list(self.master.rel.to.objects.all())
		self._show_assoc_options(associations)

		for v in slave_iterable: # Can be QS or values_list depending on above.
			if hasattr(self.parent,'clean_%s' % self.master_field_name):
				v = getattr(self.parent,'clean_%s' % self.master_field_name)(v,None)
			v = str(v).lower()
			if not v in variants:
				variants[v], associations = self._record_associate(v,associations)

		# Now update the global variants
		self._global_variants(self.master.rel.to).update(variants)

	def _show_assoc_options(self,associations):
		if not self.test:
			count = 1
			for i in associations:
				print "[%s] %s" % (count, i)
				count += 1

	def _record_associate(self,slave_value,associations):
		if not self.master.rel.to.objects.count():
			return self._record_create(slave_value), associations # There's nothing to pick from. Create one!

		while True:
			if self.test and associations:
				return associations[0], associations
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
								return
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
				import pdb;pdb.set_trace()
				return self.master.rel.to.objects.get_or_create(**new_model)[0]

	def clean(self,value):
		if self.value != None: 
			"""
			Value was explicitly set for this field importer.Field('x',value='FOO') 
			"""
			return self.value
	
		if self.map:
			"""
			Map was given, a la importer.Field('x',map={'0': False,'1': True}) 
			"""
			if value in self.map.keys():
				value = self.map[value]

		if hasattr(self,'custom_clean_function'):
			"""
			Route through custom function like imporer.Field('x',custom_function=clean_dates)
			"""
			value = self.custom_clean_function(value)

		if isinstance(self.master,ForeignKey) or isinstance(self.master,OneToOneField):
			"""
			Some core prep work for FKs. Use the variant already provided by user.
			"""
			if str(value).lower() in self._global_variants(self.master.rel.to).keys(): 
				return self._global_variants(self.master.rel.to)[str(value).lower()]
				
		elif isinstance(self.master,DecimalField):
			"""
			Respect max_value if field was decimal. TODO: Expand to INT
			"""
			if value != None:
				value = Decimal('%s' % value)
				if self.max_value and self.max_value < value:
					return self.max_value

		elif isinstance(self.master,CharField) or isinstance(self.master,TextField):
			"""
			Do some quick and dirty char encoding to make sure source data wasn't junky
			"""
			if value:
				value = '%s' % value.encode('ascii','ignore')
				if getattr(self.master,'max_length',False) and len(value) > self.master.max_length:
					return '%s' % value[:self.master.max_length]

		if not value:
			return self.if_null

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
	def __init__(self,override_values=None, queryset=None, verbose=False,slave=False,verify_by=None,parent=None,variants=None):
		print "init %s" % self.__class__.__name__
		if variants:
			self.master_variants=variants
		self.field_variants = {} 
		self.parent = parent
		if slave:
			self.Meta.slave = slave

		self.dataset = {}	# Keeps a dict of items created by. {'slave_key' : master_object }
		self.verify_by = verify_by or []
		
		self.verbose = verbose
		self.Meta.queryset = queryset
		self.override_values = override_values or {}
		self.created = []	# General numbers on the work done in this instance. 

		" Now let's make sure all req'd fields have been specified "
		for i in self.Meta.master._meta.local_fields:
			if not i.name == "id" and not i.name in [field for field in self.fields] \
					and not self.Meta.master._meta.get_field(i.name).null:
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

	def do_import(self,verify_by=None,unique=False):
		print "Importation: %s" % self.__class__.__name__
		for slave_record in self.Meta.queryset:
			new_model = None
			if verify_by: #Won't fire without imported data
				new_model = self._soft_import(slave_record,verify_by)

			if not new_model:
				new_model = self._hard_import(slave_record,unique)

			self.dataset[str(slave_record.pk)] = new_model

			if new_model and hasattr(self,'post_save'):
				self.post_save(slave_record,new_model)

		print "Processed: %s, Created: %s" % (self.Meta.queryset.count(),len(self.created))
		print "Here's a shell to inspect what was created, or not"
		import pdb;pdb.set_trace()
		self.created = None

	def _soft_import(self,slave_record,verify_by):
		#Used to match a table only by one field
		#Obviously this *NEVER* fires unless imported data exists

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
		#Regular import. Matches two tables by all fields.
		cleaned_data = {}
		for k, field in self.fields.iteritems():
			value = None
			if field.slave_field_name: # Field may not exist in slave table 
				value = getattr(slave_record,field.slave_field_name)

			if value in self.override_values.keys():
				value = self.override_values[k]
			else:
				value = self.fields[k].clean(value)

			cleaned_data[k] = value

			if hasattr(self, 'clean_%s' % k): # use custom clean_FOO override
				cleaned_data[k] = getattr(self,'clean_%s' % k)(slave_record,cleaned_data)


		if hasattr(self,'clean'):
			cleaned_data = self.clean(slave_record,cleaned_data)

		if not cleaned_data: # There was nothing to create!
			return

		return self._hard_import_commit(cleaned_data,unique)

	def _hard_import_commit(self,cleaned_data,unique):
		try:
			new_model, created = self.Meta.master.objects.get_or_create(**cleaned_data)
			if created:
				self.created.append(new_model) 
			return new_model

		except self.Meta.master.MultipleObjectsReturned:
			"""
			When re-running, we want to avoid re-creating records.
			However, here, two imported models turned out identical. 
			Because other records may OneToOne to this one, we must
			somehow respect the existance of two identical models 
			and reliably match up OneToOnes after all is said and done.

			For now we use the first of a QuerySet of those models, 
			excluding those that have already been used!

			Potential bug here is if many different models OneToOne 
			with this model. In this case, its liable to pick up one 
			of the other model's OneToOne relationships.

			Oh, and by the way, as of 6/23/09 I have no idea how, why,
			or what this is supposed to accomplish.
			"""
			if not unique:
				raise self.Meta.master.MultipleObjectsReturned

			pks_so_far = [i.pk for i in self.dataset.values()]

			# Excluding records already created this run, we return the next in line.
			try: # Get the next unclaimed copy
				return self.Meta.master.objects.exclude(pk__in=pks_so_far).filter(**cleaned_data)[0]
			except: # No unclaimed copies exist. Create a new one!
				return self.Meta.master.objects.create(**cleaned_data)

		except IntegrityError, e: 
			# Hate this next line. Probably MySQL only. Sorry!
			try:
				match = re.search("Duplicate entry '(\d+)'",e.args[1])
				errant_pk = int(match.groups(0)[0])
			except:
				import pdb;pdb.runcall(self._escape,cleaned_data,e)


			# While lots of things can cause dupes, we're only here to fix OneToOne
			one_to_one_fields = [f for f in self.Meta.master._meta.fields \
					if isinstance(f,OneToOneField)]

			culprit = []

			# Grab that unlikely sucker
			for d in cleaned_data:
				for f in one_to_one_fields:
					if d == f.verbose_name and cleaned_data[d].pk == errant_pk:
						culprit.append((d,f))

			if len(culprit) != 1:
				import pdb;pdb.set_trace()
				raise Exception("Hell appears to have frozen over")

			errant_model, errant_field = culprit[0]

			try:
				pks_so_far = [i.pk for i in self.dataset.values()]
			except:
				import pdb;pdb.set_trace()

			cleaned_data_nodupes = cleaned_data.copy()

			del cleaned_data_nodupes[errant_field.verbose_name]
			# First, let's see if this model has already been imported and has its own correct errant model
			if self.Meta.master.objects.filter(**cleaned_data_nodupes).exclude(pk__in=pks_so_far).count() == 1:
				correct_dupe = getattr(self.Meta.master.objects.exclude(pk__in=pks_so_far).get(**cleaned_data_nodupes),f.verbose_name)
				cleaned_data[f.verbose_name] = correct_dupe
				# Recurse. Maybe there's other issues to solve.
				return self._hard_import_commit(cleaned_data,unique)

			# Ahh, ok. We clearly just need a new copy of the errant model
			cleaned_data[errant_field.verbose_name].pk = None
			cleaned_data[errant_field.verbose_name].save()

			# Recurse. Maybe there's other issues to solve.
			return self._hard_import_commit(cleaned_data,unique)

		except Exception, e:
			import pdb;pdb.runcall(self._escape,cleaned_data,e)

	def _escape(self,cleaned_data,e):
		print '==ERR0R=ADDING=MODEL=======(Probably your fault)==========='
		print e
		print '==Data that failed:'

		print '%s' % cleaned_data
		print '===========You\'re in shell! Find out why!================='
	
class Import(BaseImport):
	__metaclass__ = DeclarativeFieldsMetaclass
