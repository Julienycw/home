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

serialNumber = 'SN40030015'  #'SN40030054'  # 'SN40030249'  'SN40030055'
equipmentPartId = 'bd7215b0-5dac-11ea-9998-734e7eeb71a2'  # 'c82df490-9f26-11ea-9998-734e7eeb71a2' '34a20300-b070-11ea-85a8-e9c7c5575b87'
plot_dir = './plot/post_processing_'+serialNumber
framerate = 2560

# data_file = r'E:\Julienyang\2-motor code\data_processing\data\jiadian\0703/40030015_data.pkl'
data_file = r'E:\Julienyang\3-project code\optimizer_condition/40030015.pkl'
limit_dw = 1000

url = os.getenv('NEBULA_AI_PG_READ_URL') or\
	  'postgresql://yangchenwang:uJVVKTTNJkFqBkxuW6Dp9y7m@pgm-2ze587qcqvvo29xylo.pg.rds.aliyuncs.com:3433/platform-ai'

pg_dw = pg.PG(url)


def read_condition(sn, eqId):
	sql_dw = """ SELECT DISTINCT "label" ->> 'condition'
	FROM "Point"
	WHERE "node"->>'serialNumber'=%s AND "equipment"->>'equipmentId'=%s
	AND "label" ->> 'condition' != 'None'
	"""
	conds = pg_dw.query(sql_dw, (sn, eqId))
	conditions = [v for x in conds for k, v in x.items()]
	print(conditions)
	return conditions


def read_cond_data(sn, eqId, conditions, num_per_cond):
	data = []
	for cond in conditions:
		sql_dw = """
		SElECT * FROM "Point"
		WHERE "node"->>'serialNumber'=%s AND "equipment"->>'equipmentId'=%s
		AND "label" ->> 'condition' = %s AND "timestamp" < '2020-06-24 00:00:00'
		ORDER BY "timestamp" DESC 
		LIMIT %s
		"""
		_data = pg_dw.query(sql_dw, (sn, eqId, cond, num_per_cond))
		print(f'Condition: {cond}, number: {num_per_cond}')
		data += _data
	return data


def get_peaks(data, threshold=0.1):
	data = np.asarray(data)
	inds = []
	values = []
	for i in range(len(data)-5):
		if data[i] < data.max()*threshold:  # or data[i]==data.max():
			continue
		low = max(i-5, 0)
		high = min(i+5, len(data))
		if data[i] == data[low:high].max():
			values.append(data[i])
			inds.append(i)
	return inds, values


def iomega(acc, fs, trans=-1):
	N = len(acc)
	df = 1 / (N * (1 / fs))
	nyq = 1 / (2 * (1 / fs))
	array = np.arange(-nyq, nyq, df)
	io = 1j * 2 * np.pi
	iomega_array = io * array
	iomega_exp = trans

	ACC = np.fft.fft(acc)
	idx = int(N / 2)
	ACC1 = ACC[:idx]
	ACC2 = ACC[idx:]
	ACC_New = np.hstack((ACC2, ACC1))

	for i in range(N):
		if iomega_array[i] != 0:
			ACC_New[i] = ACC_New[i] * (iomega_array[i] ** iomega_exp)
		else:
			ACC_New[i] = complex(0.0, 0.0)

	ACCN1 = ACC_New[:idx]
	ACCN2 = ACC_New[idx:]
	ACCF = np.hstack((ACCN2, ACCN1))

	accf = np.fft.ifft(ACCF)

	final = []
	for j in range(N):
		real_num = accf[j].real
		final.append(real_num)

	return np.array(final)


def IntFcn(acc, fs=2560, trans=-1):
	lint = iomega(acc, fs, trans=trans)
	if trans == -1:
		vebration = signal.detrend(lint)
		final = vebration
	else:
		x = np.arange(len(acc))
		p = np.polyfit(x, lint, 2)
		drop = np.polyval(p, x)
		final = lint - drop

	return final


def get_freq_range(speed, n_fft=4096, sr=2560):
	# last version [0, 0.5, 1, 2, 3, 8, 20]
	freq_range = np.array([0.4, 1, 2, 3, 5, 8, 12, 16, 18, 20, 22, 24])
	# freq to bin
	f0 = speed / 60.
	f0_id = f0 * n_fft / sr
	freq_range *= f0_id
	freq_range = freq_range.astype(np.int)
	return freq_range, f0_id


def plot_data(_data, index):
	time = str(_data['timestamp']).split('+')[0]
	label = _data['label']['condition']
	acc = _data['sensor']['acceleration']['y']
	acc = np.asarray(acc)
	acc = acc - acc.mean()

	fft_x = [i * framerate/len(acc) for i in range(1, len(acc)//2+1)]
	fft_x = np.asarray(fft_x)
	acc_fft_y = np.abs(np.fft.rfft(acc)[1:])
	acc_inds, acc_peaks = get_peaks(acc_fft_y, threshold=0.1)

	v = IntFcn(acc, fs=framerate, trans=-1)
	v_fft_y = np.abs(np.fft.rfft(v)[1:])
	v_inds, v_peaks = get_peaks(v_fft_y, threshold=0.05)

	freq_range, f0_id = get_freq_range(1475)

	plt.figure()
	plt.subplot(211)
	plt.plot(fft_x, acc_fft_y, label='acc')
	plt.scatter(fft_x[np.asarray(acc_inds)], acc_peaks, c='tab:orange', marker='x')
	plt.legend()
	plt.title(serialNumber)
	plt.subplot(212)
	plt.plot(fft_x[16:], v_fft_y[16:], label='v')
	plt.scatter(fft_x[np.asarray(v_inds)][1:], v_peaks[1:], c='tab:orange', marker='x')
	for freq in freq_range:
		plt.plot([freq/4096*2560]*2, [0, 0.5], c='tab:blue', linestyle=':')
	plt.legend()
	plt.title(f'Index_{index} {time}-{label}')
	plt.tight_layout()
	file_name = f'Index {index}-{label}-{time.replace(":", "_")}.png'
	os.makedirs(plot_dir, exist_ok=True)
	plt.savefig(os.path.join(plot_dir, file_name), format='png', bbox_inches='tight')


def main():
	# # read online data
	# conditions = read_condition(sn=serialNumber, eqId=equipmentPartId)
	# data = read_cond_data(sn=serialNumber, eqId=equipmentPartId, conditions=conditions, num_per_cond=20)

	# read offline data
	data = pickle.load(open(data_file, 'rb'))
	for i, _data in enumerate(data):
		plot_data(_data, index=i)


if __name__ == '__main__':
	main()
