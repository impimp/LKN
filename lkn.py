#!/usr/bin/python3
import json
import re
from subprocess import PIPE, Popen
#import psycopg2
from datetime import datetime
import argparse
import binascii

class Modifiers:

	def evaluate(modifier, input):
		if modifier == 'kbytes_to_bytes':
			return Modifiers.kbytes_to_bytes(input)

	def kbytes_to_bytes(input):
		return input*1024

class RuleSet:
	def __init__(self):
		self._rules=[]
		self._resultSet = {}

	def append(self, rule):
		self._rules.append(rule)

	def evaluate(self, input):
		for key in self._rules:

			#get start/end from the stream
			input_ = json.loads(input)
			start_ = input_['start']
			end_ = input_['end']

			#if end_-start_ > 1000:
			#	#@TODO@
			#	# What to do with the samples here?

			del input_['start']
			del input_['end']
			
			key.loadInput(input_)
			key.evaluate()

			self._resultSet.update({'start': start_, 'end': end_, 'data': key.getResultSet()})
			key.clean()

	def getResultSet(self):
		return self._resultSet

	def clean(self):
		self._resultSet={}

class Rule:
	def __init__(self):
		self._result = {}
		self._labels = {}

	def load(self, filename):
		fp = open(filename)
		self._rule = json.load(fp)
		fp.close()

	def loads(self, string):
		self._rule = json.loads(filename)

	def loadInput(self, input):
		self._input = input

	def hashDict(labels):
		labels_ = str(sorted(labels.items(), key=lambda kv: kv[1]))
		return binascii.crc32(labels_.encode('utf8'))

	def createMetric(self, name, value, labels):
		labels={**labels}
		key = "{}:{}".format(name, Rule.hashDict(labels))
		self._result[key] = { 'data': value, 'labels': labels }
		
	def updateMetric(self, name, value, labels):
		labels={**labels}
		key = "{}:{}".format(name, Rule.hashDict(labels))
		self._result[key]['data'] +=  value
		#self._result[name]['labels'].update(labels)

	def _evaluateModifiers(self, input, modifiers):
		i_ = input
		
		if modifiers is None:
			return input
		for mod in modifiers:
			i_ = Modifiers.evaluate(mod, i_)
		return i_

	def _evaluateSimple(self):
		labels_ = {}

		for key in self._input:
			if key == self._rule['simple']['pattern']:
				value_ = self._evaluateModifiers(self._input[key], self._rule['simple'].get('modifiers'))
				
				if "labels" in self._rule['simple']:
					labels_.update(self._rule['simple']['labels'])
				self.createMetric(self._rule['simple']['replacement'], value_, labels_)
				return 1
		return 0

	def _evaluateRegexp_Group(self):
		c = 0
		labels_ = {}

		for key in self._input:
			if re.match(self._rule['regexp_group']['pattern'], key):
				value_ = self._evaluateModifiers(self._input[key], self._rule['regexp_group'].get('modifiers'))

				if "labels" in self._rule['regexp_group']:
					for label,label_pattern in self._rule['regexp_group']['labels'].items():
						if not label in self._labels:
							label_value = re.sub(self._rule['regexp_group']['pattern'], label_pattern, key)
							labels_[label] = label_value

				try:
					self.updateMetric(self._rule['regexp_group']['replacement'], value_, labels_)
				except KeyError:
					self.createMetric(self._rule['regexp_group']['replacement'], value_, labels_)

				c = c + 1
		return c

	def _evaluateRegexp_Replace(self):
		c = 0
		labels_ = {}

		for key in self._input:
			if re.match(self._rule['regexp_replace']['pattern'], key):
				name_ = re.sub(self._rule['regexp_replace']['pattern'], self._rule['regexp_replace']['replacement'], key)
				value_ = self._evaluateModifiers(self._input[key], self._rule['regexp_replace'].get('modifiers'))
				
				if "labels" in self._rule['regexp_replace']:
					for label,label_pattern in self._rule['regexp_replace']['labels'].items():
						if not label in self._labels:
							label_value = re.sub(self._rule['regexp_replace']['pattern'], label_pattern, key)
							labels_[label] = label_value

				self.createMetric(name_, value_, labels_)
				c = c + 1
		return c

	def _evaluateExternal(self):
		#
		# @TODO@
		# 
		# This will be implemented in a later stage of the project.
		# For now, that's not really needed.
		# 
		# Method `external` is de facto a scripting support.
		#
		return 0


	def evaluate(self):
		# 0. It expects the input which is a dict.
		# 1. Firstly it iteratest and evaluates the simple replacement, if the 
		#    rule has any.
		#    
		#    
		self.clean()
		count = 0

		if "simple" in self._rule:
			count += self._evaluateSimple()
		if "regexp_group" in self._rule:
			count += self._evaluateRegexp_Group()
		if "regexp_replace" in self._rule:
			count += self._evaluateRegexp_Replace()
		if "external" in self._rule:
			count += self._evaluateExternal()

		return count

	def getResultSet(self):
		return self._result

	def clean(self):
		self._result={}
		self._labels={}


class Metrics:
	def __init__(self, ftdc_parser):
		self._ftdc_parser = ftdc_parser

	def setMetricsFile(self, metrics_file):
		self._metrics_file = metrics_file

	def appendRule(self, rule):
		pass

	def parse(self):
		cmd = "{} export {} -o /dev/stdout -s".format(self._ftdc_parser, self._metrics_file)

		p = Popen(cmd, shell=True, bufsize=4096, stdin=PIPE, stdout=PIPE, close_fds=True)
		while True:
			line = p.stdout.readline()
			yield line.decode("utf-8").rstrip()
			is_EOF = p.stdout.peek(1)
			if len(is_EOF) == 0:
				break;
		try:
			p.kill()
		except OSError:
			pass

class Sink:
	def __init__(self):
		self._labels = {}

	def setLabel(self, name, label):
		self._labels[name] = label

	def save(self, input):
		self.saveHandlerBegin()
		self.saveHandler('up',
			','.join('{}="{}"'.format(key, value) for key, value in self._labels.items()),
			input['start'],
			1)
		
		for metric in input['data']:
			l_={**input['data'][metric]['labels'], **self._labels}
			labels_ = ','.join('{}="{}"'.format(key, value) for key, value in l_.items())
			metric_ = re.sub(r"(.*):.*", r"\1", metric)
			self.saveHandler(metric_,
				labels_,
				input['start'],
				input['data'][metric]['data'])
		self.saveHandlerCommit()


class PostgreSQLSink(Sink):
	def __init__(self, hostname, username, password, schemaname):
		super().__init__()
		self.conn = psycopg2.connect("host='{}' user='{}' dbname='{}' password='{}'".format(hostname, username, schemaname, password))

	def saveHandler(self, metricName, labels, timestamp, value):
		cur = self.conn.cursor()
		cur.execute("INSERT INTO metrics VALUES ('{}{{{}}} {} {}')".format(metric, labels_, input['data'][metric]['data'], input['start']))
		self.conn.commit()

	def saveHandlerCommit(self):
		pass

	def saveHandlerBegin(self):
		pass

class PrintSink(Sink):
	def __init__(self):
		super().__init__()

	def saveHandlerCommit(self):
		pass

	def saveHandlerBegin(self):
		pass

	def saveHandler(self, metricName, labels, timestamp, value):
		
		print ("---\n\tmetricName={}\n\tlabels={}\n\tttimestamp={} ({})\n\tvalue={}\n---\n".format(
				metricName,
				labels,
				timestamp,
				datetime.fromtimestamp(timestamp/1000),
				value
			))


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-p", "--parser", type=str, help="FTDC parser to use", required=True)
	parser.add_argument("-r", "--rule", type=str, help="Rule file", required=True)
	parser.add_argument("-m", "--metrics", type=str, help="Metrics file", required=True)
	args = parser.parse_args()

	r_ = Rule()
	r_.load(args.rule)
	RulesSets = RuleSet()
	RulesSets.append(r_)

	sink = PrintSink()
	metr = Metrics(args.parser)
	metr.setMetricsFile(args.metrics)
	for l in metr.parse():
		RulesSets.evaluate(l)
		res = RulesSets.getResultSet()
		sink.save(res)
		RulesSets.clean()
