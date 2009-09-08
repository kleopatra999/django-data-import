from django.core.management.base import BaseCommand
from optparse import make_option

class Command(BaseCommand):
	args = ['conversion class...']

	def handle(self, conversion_class, **options):
		conversion = __import__('conversion.imports')
		getattr(getattr(conversion,'imports'),conversion_class)()
