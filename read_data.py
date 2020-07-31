import os
import driver.pg as pg
import numpy as np
from scipy import signal
import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib import pyplot as plt
import pickle
import sys
sys.path.insert(0, './driver')

serialNumber = 'SN40030024'  #'SN40030054' 'SN40030055'
equipmentPartId = 'bd7215b0-5dac-11ea-9998-734e7eeb71a2'  # '34a20300-b070-11ea-85a8-e9c7c5575b87'
plot_dir = './plot/post_processing_'+serialNumber
framerate = 2560

limit_dw = 1000

url = os.getenv('NEBULA_AI_PG_READ_URL') or\
	  'postgresql://yangchenwang:uJVVKTTNJkFqBkxuW6Dp9y7m@pgm-2ze587qcqvvo29xylo.pg.rds.aliyuncs.com:3433/platform-ai'

pg_dw = pg.PG(url)


def read_condition(sn, eqId):
	sql_dw = """
	SELECT DISTINCT "label" ->> 'condition'
	FROM "Point"
	WHERE "node"->>'serialNumber'=%s AND "equipment"->>'equipmentId'=%s
	AND "label" ->> 'condition' != 'None'
	"""
	conds = pg_dw.query(sql_dw, (sn, eqId))
	conditions = [v for x in conds for k, v in x.items()]
	print(conditions)
	return conditions


def read_cond_data(sn, eqId, conditions):
	data = []
	for cond in conditions:
		sql_dw = """
		SElECT * FROM "Point"
		WHERE "node"->>'serialNumber'=%s AND "equipment"->>'equipmentId'=%s
		AND "label" ->> 'condition' = %s AND "timestamp" < '2020-06-24 00:00:00'
		ORDER BY "timestamp" DESC 
		LIMIT %s
		"""
		_data = pg_dw.query(sql_dw, (sn, eqId, cond, limit_dw))
		interrupt = len(_data) // 3
		_data = _data[:-interrupt]
		print(f'Condition: {cond}, number: {len(_data)}')
		data += _data
	return data


def read_data():
	sn = 'SN40030055'
	eqId = '34a20300-b070-11ea-85a8-e9c7c5575b87'
	sql_dw = """
	SElECT * FROM "Point"
	WHERE "node"->>'serialNumber'=%s AND "equipment"->>'equipmentId'=%s
	AND "label" ->> 'condition' != %s AND "timestamp" > '2020-06-24 00:00:00'
	ORDER BY "timestamp" DESC 
	LIMIT %s
	"""
	data = pg_dw.query(sql_dw, (sn, eqId, 'None', limit_dw))
	print(f'Data: {len(data)}')
	return data


def main():
	conditions = read_condition(sn=serialNumber, eqId=equipmentPartId)
	data = read_cond_data(sn=serialNumber, eqId=equipmentPartId, conditions=conditions)
	pickle.dump(data, open(f'./{serialNumber[2:]}.pkl', 'wb'))

	# data = read_data()
	# pickle.dump(data, open('./40030055.pkl', 'wb'))


if __name__ == '__main__':
    main()
