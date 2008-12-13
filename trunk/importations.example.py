from datetime import datetime

from django.contrib.auth.models import User
from django.forms.models import model_to_dict

import importer

from coms.core.consumer import *

from coms.core.base.models import Address
from coms.core.base.models import UserProfile
from coms.core.base.models import PhoneType
from coms.core.base.models import Relationship

from project.dummy.models import *

blank_date = datetime(2007,1,20,01,01,01)   # It is very important to use a static date to fill in gaps
old_date = datetime(1900,11,20,01,01,01)    # Dynamic dates will create duplicate records since get_or_create will always 'create'

class UserImport(importer.Import):
   username = importer.Field('username')
   email = importer.Field('email')
   first_name = importer.Field('firstname')
   last_name = importer.Field('lastname')
   is_active = importer.Field('emptype')
   password = importer.Field(None,value='tmp')
   is_staff = importer.Field(None,value=False)
   is_superuser = importer.Field(None,value=False)
   last_login = importer.Field(None,value=blank_date)
   date_joined = importer.Field(None,value=blank_date)

   def clean_email(self,slave_record,cleaned_data):
      return '%s%s@old_site.com' % (slave_record.firstname.lower(),slave_record.lastname.lower())

   def clean_username(self,slave_record,cleaned_data):
      return slave_record.username.lower()

   def clean_is_active(self,slave_record,cleaned_data):
      if slave_record.emptype != "UNEMP":
         return True
      return False

   class Meta:
      master = User
      slave = OldUsers.objects.all()

class UserProfileImport(importer.Import):
   user = importer.Field('username',fires_import=UserImport,verify_by=('username',))
   title = importer.Field('title')
   location = importer.Field('office')
   supervisor = importer.Field('supervisorid',fires_import=UserImport)
   exempt = importer.Field(None,value=False)
   annual_pto = importer.Field(None,value=0)

   def clean_supervisor(self,slave_record,cleaned_data):
      if User.objects.filter(username=slave_record.username.lower()).count():
         return User.objects.get(username=slave_record.username.lower())
      return User.objects.get(username='dmoore')

   def clean_user(self,slave_record,cleaned_data):
      return User.objects.get(username=slave_record.username.lower)

   class Meta:
      master = UserProfile
      slave = OldUsers.objects.all()

class AddressImport(importer.Import):
   line_1 = importer.Field('address1',if_null='Unknown')
   line_2 = importer.Field('address2')
   city = importer.Field('city',if_null='Unknown')
   state = importer.Field('state',if_null='FL',max_length=2)
   zip = importer.Field('zip',if_null='00000')
   mail_opt_out = importer.Field('mailoptout',if_null=False)
   newsletter = importer.Field('mailinglist',if_null=False)
   phone_primary = importer.Field('phoneprimary',if_null='000-000-0000',max_length=20)
   phone_primary_type = importer.Field('phoneprimarytype')
   phone_secondary = importer.Field('phonesecondary',max_length=20)
   phone_secondary_type = importer.Field('phonesecondarytype')
   email = importer.Field('email')
   preferred = importer.Field('preferred')
   bad_address = importer.Field(None,if_null=False)
   def clean_line_2(self,slave_record,cleaned_data):
      if 'line_2' in cleaned_data and cleaned_data['line_2'] == 'None':
         return None
      return cleaned_data['line_2']

class EAddressImport(AddressImport):
   class Meta:
      master = Address
      slave = OldAddresses.objects.filter(id__in=OldCustomers.objects.values_list('eaddressid',flat=True).distinct())

class E2AddressImport(AddressImport):
   class Meta:
      master = Address
      slave = OldAddresses.objects.filter(id__in=OldCustomers.objects.values_list('e2addressid',flat=True).distinct())

class CAddressImport(AddressImport):
   class Meta:
      master = Address
      slave = OldAddresses.objects.filter(id__in=OldCustomers.objects.values_list('addressid',flat=True).distinct())


class EContactImport(importer.Import):
   first_name = importer.Field(None)
   last_name = importer.Field(None)

   def clean_address(self,slave_record,cleaned_data):
      if hasattr(slave_record,'eaddressid'):
         pk = slave_record.eaddressid
      else:
         pk = slave_record.e2addressid

      if hasattr(slave_record,'econtact'):
         name_field = slave_record.econtact
      else:
         name_field = slave_record.e2contact

      if not name_field:
         return None 

      if not 'address' in cleaned_data or not cleaned_data['address']:
         return Address.objects.get_or_create(line_1="Autocreated for missing %s" % pk,city='Tallahassee',state='FL',\
               zip='00000',phone_primary='999-999-9999',phone_primary_type=PhoneType.objects.get(name='voice'))[0]
      return cleaned_data['address']

   def clean(self,slave_record,cleaned_data):
      if hasattr(slave_record,'econtact'):
         name_field = slave_record.econtact
      else:
         name_field = slave_record.e2contact

      if not name_field:
         return None 

      test_name = name_field.split(' ',1)

      if len(test_name) > 0:
         cleaned_data['first_name'] = test_name[0]
         cleaned_data['last_name'] = test_name[1]
      else:
         cleaned_data['first_name'] = test_name
         cleaned_data['last_name'] = 'Unknown' 
      return cleaned_data


class EmergencyContactPrimaryImport(EContactImport):
   relationship = importer.Field('erel')
   address = importer.Field('eaddressid',fires_import=EAddressImport)
   class Meta:
      master = EmergencyContact
      slave = OldCustomers.objects.filter(eaddressid__isnull=False)

class EmergencyContactSecondaryImport(EContactImport):
   address = importer.Field('e2addressid',fires_import=E2AddressImport)
   relationship = importer.Field('e2rel')

   class Meta:
      master = EmergencyContact
      slave = OldCustomers.objects.filter(e2addressid__isnull=False)

class CustomerImport(importer.Import):
   staff = importer.Field('addedby',fires_import=UserImport,verify_by=('username',))
   date_added = importer.Field('timestamp',if_null=old_date)
   file = importer.Field('consumerid')
   first_name = importer.Field('firstname',if_null='Unknown')
   last_name = importer.Field('lastname',if_null='Unknown')
   address = importer.Field('addressid',fires_import=CAddressImport)
   middle_initial = importer.Field('mi')
   birth_date = importer.Field('birthdate',if_null=blank_date)
   entry_date = importer.Field('entrydate',if_null=blank_date)
   closure_date = importer.Field('closuredate')
   ssn = importer.Field('ssn',if_null='0000')
   status = importer.Field('status')
   waive_ilp = importer.Field('createilp',if_null=False)
   emergency_contact_primary = importer.Field(None,fires_import=EmergencyContactPrimaryImport)
   emergency_contact_secondary = importer.Field(None,fires_import=EmergencyContactSecondaryImport)
   disability_primary = importer.Field('disabprimary')
   disability_secondary = importer.Field('disabsecondary')
   language_primary = importer.Field('langprimary')
   language_secondary = importer.Field('langsecondary')
   ethnicity = importer.Field('ethnicity')
   race = importer.Field('race')
   gender = importer.Field('gender')
   employment_status = importer.Field('empstatus')
   household_type = importer.Field('householdtype')
   household_annual_income = importer.Field('householdannual')
   crime_victim = importer.Field('crimevictim',if_null=False)
   is_veteran = importer.Field('isvet',if_null=False)

   def clean_staff(self,slave_record,cleaned_data):
      if OldUsers.objects.filter(id=slave_record.addedby).count():
         return User.objects.get(username=OldUsers.objects.get(id=slave_record.addedby).username.lower())
      return User.objects.get(username='coms')

   def clean_address(self,slave_record,cleaned_data):
      pk = slave_record.addressid
      if not 'address' in cleaned_data or not cleaned_data['address']:
         return Address.objects.get_or_create(line_1="Autocreated for missing %s" % pk,city='Tallahassee',state='FL',\
               zip='00000',phone_primary='999-999-9999',phone_primary_type=PhoneType.objects.get(name='voice'))[0]
      same_address = Customer.objects.filter(address=cleaned_data['address']).exclude(first_name=cleaned_data['first_name'],last_name=cleaned_data['last_name'])
      if same_address.count():
         cleaned_data['address'].pk = None
         cleaned_data['address'].save()
      return cleaned_data['address']

   def post_save(self,slave_record,master_record):
      if slave_record.householdtype == 'Female head of household':
         CustomerTag.objects.get_or_create(consumer=master_record,type=Tag.objects.get_or_create(name="Female head of household")[0])

   class Meta:
      master = Customer 
      slave = OldCustomers.objects.all()

class GoalImport(importer.Import):
   start_date = importer.Field('startdate')
   type = importer.Field('goal')
   notes = importer.Field('description',if_null='')
   consumer = importer.Field('consumerid',fires_import=CustomerImport,verify_by=('file','consumerid'))
   target_date = importer.Field('targetdate',if_null=blank_date)
   status = importer.Field('status')
   resolution_date = importer.Field('statusdate')
   date_added = importer.Field('timestamp')
   staff = importer.Field('addedby',fires_import=UserImport,verify_by=('username',))
   def clean_staff(self,slave_record,cleaned_data):
      if not cleaned_data['staff'] or not OldUsers.objects.filter(pk=slave_record.addedby).count():
         return User.objects.get(username='COMS')
      return cleaned_data['staff']

   class Meta:
      master = Goal 
      slave = CustomerGoals.objects.all()

class ServiceImport(importer.Import):
   start_date = importer.Field('startdate',if_null=old_date)
   type = importer.Field('service')
   notes = importer.Field('description',if_null='')
   target_date = importer.Field('targetdate',if_null=blank_date)
   status = importer.Field('status')
   resolution_date = importer.Field('statusdate')
   goal = importer.Field('goalid',fires_import=GoalImport)
   date_added = importer.Field('timestamp')
   staff = importer.Field('addedby',fires_import=UserImport,verify_by=('username',))

   def clean_staff(self,slave_record,cleaned_data):
      if not cleaned_data['staff'] or not OldUsers.objects.filter(pk=slave_record.addedby).count():
         return User.objects.get(username='COMS')
      return cleaned_data['staff']

   class Meta:
      master = Service
      slave = CustomerServices.objects.all()

class CustomerContactImport(importer.Import):
   date = importer.Field('contactdate',if_null=old_date)
   notes = importer.Field('comments',if_null='')
   consumer = importer.Field('consumerid',fires_import=CustomerImport,verify_by=('file','consumerid'))
   program = importer.Field('program')
   hours = importer.Field('hours',if_null=0)
   staff = importer.Field('addedby',fires_import=UserImport,verify_by=('username',))
   service = importer.Field('service',fires_import=ServiceImport)
   group = importer.Field('groupname')
   date_added = importer.Field('timestamp')
   def clean_staff(self,slave_record,cleaned_data):
      if not cleaned_data['staff'] or not OldUsers.objects.filter(pk=slave_record.addedby).count():
         return User.objects.get(username='COMS')
      return cleaned_data['staff']
   
   def clean(self,slave_record,cleaned_data):
      consumer =  Customer.objects.get(file=OldCustomers.objects.get(id=slave_record.consumerid).consumerid)
      cleaned_data['consumer'] = consumer
      return cleaned_data

   def post_save(self,slave,master):
      if master.group:
         for i in CustomerGroups.objects.filter(consumerid=slave.consumerid,group=slave.groupname):
            master.group.consumers.add(master.consumer)
         master.service.group = master.group
         master.service.save()

   class Meta:
      master = Contact
      slave = CustomerContacts.objects.all()
