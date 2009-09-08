from django.core.management.base import BaseCommand
from optparse import make_option

class Command(BaseCommand):
	args = ['conversion class...']

	def handle(self, conversion_class, **options):
		imports = __import__('conversion.imports')
		conversion = getattr(imports,conversion_class)
		conversion()
