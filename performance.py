from classes import *
from POC import db_connection, db_engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import datetime


def trackDataFrame(track, batchID):
	"""
	:param track: The object containing all the information to keep track of
	:return: Makes a dataframe that contains the information in track
	"""
	new_row = {'Machine': "", 'Time Full': "", 'Time Empty': ""}  # Option that less than 4 machines used
	track.machines_time = track.machines_time.append(new_row,
													 ignore_index=True)  # Add 4 extra rows to be sure there are enough
	track.machines_time = track.machines_time.append(new_row, ignore_index=True)
	track.machines_time = track.machines_time.append(new_row, ignore_index=True)
	track.machines_time = track.machines_time.append(new_row, ignore_index=True)

	df_machines = track.machines_time
	workcenter = df_machines['Machine']
	full = df_machines['Time Full']
	empty = df_machines['Time Empty']
	df = pd.DataFrame(
		columns=['Machines Usage', 'Total Machine Time Full', 'Total Machine Time Empty', 'Amount of Orders Planned',
				 'Amount of Orders Not Planned', 'Amount of orders planned overtime',
				 'Amount of Item numbers not in the Database',
				 'Machines that can be Used', 'Color change Severity',
				 'Conflicted Mould changes'])  # MAke the dataframe

	new_row = {
		'Machines Usage': "Machine " + str(workcenter[0]) + ": Time full: " + str(full[0]) + " Time empty: " + str(
			empty[0]),
		'Total Machine Time Full': "Days: " + str(track.total_machine_time_full[0]),
		'Total Machine Time Empty': "Days: " + str(track.total_machine_time_over[0]),
		'Amount of Orders Planned': str(track.orders_planned),
		'Amount of Orders Not Planned': str(track.orders_not_planned),
		'Amount of orders planned overtime': str(track.orders_overtime),
		'Amount of Item numbers not in the Database': str(track.items_not_in_database),
		'Machines that can be Used': str(len(track.available_machines)) + " machines:",
		'Color change Severity': str(track.color_severity[0][0]) + ": " + str(track.color_severity[0][1]),
		'Conflicted Mould changes': str(track.conflicted_mould)}
	df = df.append(new_row, ignore_index=True)  # Add the row

	new_row = {
		'Machines Usage': "Machine " + str(workcenter[1]) + ": Time full: " + str(full[1]) + " Time empty: " + str(
			empty[1]),
		'Total Machine Time Full': "Hours: " + str(track.total_machine_time_full[1]),
		'Total Machine Time Empty': "Hours: " + str(track.total_machine_time_over[1]),
		'Machines that can be Used': str(track.available_machines),
		'Color change Severity': str(track.color_severity[1][0]) + ": " + str(track.color_severity[1][1])}
	df = df.append(new_row, ignore_index=True)

	new_row = {
		'Machines Usage': "Machine " + str(workcenter[2]) + ": Time full: " + str(full[2]) + " Time empty: " + str(
			empty[2]),
		'Total Machine Time Full': "Minutes: " + str(track.total_machine_time_full[2]),
		'Total Machine Time Empty': "Minutes: " + str(track.total_machine_time_over[2]),
		'Color change Severity': str(track.color_severity[2][0]) + ": " + str(track.color_severity[2][1])}
	df = df.append(new_row, ignore_index=True)

	new_row = {
		'Machines Usage': "Machine " + str(workcenter[3]) + ": Time full: " + str(full[3]) + " Time empty: " + str(
			empty[3]),
		'Total Machine Time Full': "Seconds: " + str(track.total_machine_time_full[3]),
		'Total Machine Time Empty': "Seconds: " + str(track.total_machine_time_over[3]),
		'Color change Severity': str(track.color_severity[3][0]) + ": " + str(track.color_severity[3][1])}
	df = df.append(new_row, ignore_index=True)

	for i in range(4, len(workcenter)):
		new_row = {
			'Machines Usage': "Machine " + str(workcenter[i]) + ": Time full: " + str(full[i]) + " Time empty: " + str(
				empty[i])}
		df = df.append(new_row, ignore_index=True)
	# Visualize performance data
	visualize(df, df_machines, batchID)
	engine = db_engine()
	# Test SQL connection
	engine.connect()
	df['batchID'] = batchID
	df.to_excel("TrackingInfo2.xlsx")
	df.to_sql("Stats", con=engine, if_exists='append', index=False)


def addMachineTrackingInfo(machines, start_date, end_date, track) :
	"""
	:param machines: The work centers
	:param start_date: Start date of planning
	:param end_date: End date of planning
	:param track: The object to keep track of how much time each work center is used
	:return: Add the information to the object
	"""

	for m in machines:

		totaltime = end_date - start_date   	 		 # The total time in planning. Need to also multiply by avilibility %
		time_full_delta = machines[m].use_time 	         # The time that the machine is used, timedelta object
		time_empty_delta = totaltime - time_full_delta   # The time that the machine is unused, timedelta object

		# if the machine is used past the end date, time empty is set to 0
		time_empty_delta = max(time_empty_delta, datetime.timedelta(0))
		
		# get "%d %X" versions of timedelta objects
		time_full = changeTimeDeltaToStr(time_full_delta)
		time_over = changeTimeDeltaToStr(time_empty_delta)
		# add new row to track
		new_row = {'Machine': m, 'Time Full': time_full, 'Time Empty': time_over}
		track.machines_time = track.machines_time.append(new_row, ignore_index=True)

		# return [day, hr, min, sec] from timedelta string
		time_full_details = parseTimeDeltaStr(time_full)
		time_over_details = parseTimeDeltaStr(time_over)
		
		# add current machine times to total
		for i in range(4):
			track.total_machine_time_full[i] += time_full_details[i]

	# change value formatting to fit time-format
	# consider changing this to divmod
	t_day = track.total_machine_time_over[0]
	t_hour = track.total_machine_time_over[1]
	t_min = track.total_machine_time_over[2]
	t_sec = track.total_machine_time_over[3]
	while t_sec >= 60:
		t_sec -= 60
		t_min += 1
	while t_min >= 60:
		t_min -= 60
		t_hour += 1
	while t_hour >= 24:
		t_hour -= 24
		t_day += 1

	# add to track
	track.total_machine_time_over = [t_day, t_hour, t_min, t_sec]
	# sort track by empty machines
	track.machines_time = track.machines_time.sort_values(by=['Time Empty', 'Time Full'])


def changeTimeDeltaToStr(tdelta):
	""" changes timedelta object to a string
	of the form '%d %X' or '1 21:24:16' """
	tdelta_str = str(tdelta)
	# tdelta_str can take the form of "h:m:s"
	# or "d days, h:m:s"
	# so we change it to "d h:m:s"
	if len(tdelta_str)>8:
		if "days" in tdelta_str:
			tdelta_str = tdelta_str.replace(" days, ", " ")
		else:
			tdelta_str = tdelta_str.replace(" day, ", " ")
	else:
		tdelta_str = "0 " + tdelta_str

	return tdelta_str

def parseTimeDeltaStr(tdelta_str):
	""" parses timedelta string of the form '%d %X' 
		and returns list of [day, hour, min, sec] """
	split_day = tdelta_str.split(" ")
	days = int(split_day[0])

	split_time = split_day[1].split(":")
	
	hrs = int(split_time[0])
	mins = int(split_time[1])
	secs = int(split_time[2])

	return [days, hrs, mins, secs]


def visualize(df, df_machines, batchID):
	# df_machines = df_machines[df_machines['Machine'].notna()]
	df_machines['Time Full days'] = df_machines['Time Full'].str.split()
	df_machines['Time Full stamp'] = df_machines['Time Full'].str.split()
	df_machines['Time Empty days'] = df_machines['Time Empty'].str.split()
	df_machines['Time Empty stamp'] = df_machines['Time Empty'].str.split()
	df_machines['Time Full total'] = np.nan
	for index, row in df_machines.iterrows():
		if len(row['Machine']) == 0:
			# print("empty!")
			# print(row['Machine'])
			df_machines = df_machines.drop(index)
		if len(row['Time Full days']) == 2:
			df_machines.loc[index, 'Time Full days'] = int(row['Time Full days'][0])
			df_machines.loc[index, 'Time Full stamp'] = row['Time Full days'][1]
		elif len(row['Time Full days']) == 1:
			df_machines.loc[index, 'Time Full days'] = 0
			df_machines.loc[index, 'Time Full stamp'] = row['Time Full days'][0]
		else:
			df_machines.loc[index, 'Time Full days'] = 0
			df_machines.loc[index, 'Time Full stamp'] = '00:00:00'

		if len(row['Time Empty days']) == 2:
			df_machines.loc[index, 'Time Empty days'] = int(row['Time Empty days'][0])
			df_machines.loc[index, 'Time Empty stamp'] = row['Time Empty days'][1]
		elif len(row['Time Empty days']) == 1:
			df_machines.loc[index, 'Time Empty days'] = 0
			df_machines.loc[index, 'Time Empty stamp'] = row['Time Empty days'][0]
		else:
			df_machines.loc[index, 'Time Empty days'] = 0
			df_machines.loc[index, 'Time Empty stamp'] = '00:00:00'
	total_f = 0
	total_e = 0
	df_machines = df_machines.dropna(subset=['Machine'])
	# df_machines = df_machines[df_machines['Machine'].notnull()]

	for index, row in df_machines.iterrows():
		# Calculate total hours per machine filled
		stamp1 = datetime.datetime.strptime(row['Time Full stamp'], '%H:%M:%S')
		daytohour1 = (row['Time Full days'] * 24)
		hours1 = stamp1.hour + (stamp1.minute / 60) + (stamp1.second / 3600)
		df_machines.loc[index, 'Time Full total'] = daytohour1 + hours1
		# Same for empty
		stamp2 = datetime.datetime.strptime(row['Time Empty stamp'], '%H:%M:%S')
		daytohour2 = (row['Time Empty days'] * 24)
		hours2 = stamp2.hour + (stamp2.minute / 60) + (stamp2.second / 3600)
		df_machines.loc[index, 'Time Empty total'] = daytohour2 + hours2
		# Get total hours
		total_f = total_f + (daytohour1 + hours1)
		total_e = total_e + (daytohour2 + hours2)
	df_machines.to_excel('df_machine.xlsx')
	total = total_f + total_e
	total_f_p = total_f / total
	total_e_p = total_e / total

	# Now that we have the data ready to visualize call the other functions to plot 
	machinefulfillment(df_machines, batchID)
	colorseverity(df, batchID)
	plannedchart(df, batchID)
	totalfulfillment(total_f_p, total_e_p, batchID)
	conflicts(df, batchID)


def colorseverity(df, batchID):
	df_color = df[:4]
	df_color['Color'] = df_color['Color change Severity']
	df_color['No'] = df_color['Color'].str.extract('(\d+)').astype(int)
	color_index = ['Big', 'Medium', 'Small', 'None']
	df_color['Color'] = color_index
	df_color = df_color[['Color', 'No']]
	# Upload to database table (dbo.P_color)
	df_color['batchID'] = batchID
	engine = db_engine()
	df_color.to_sql("P_color", con=engine, if_exists='append', index=False)
	""" Deprecated 13/9/2020 - Implemented in vue.js
	plt.figure(figsize=(5, 5))
	plt.style.use('seaborn-darkgrid')
	plt.bar(df_color['Color'],
			df_color["No"],
			width=0.6)
	plt.ylabel("Number of cases")
	plt.title("Color change Severity")
	plt.savefig("color.jpg", dpi=150) """


def machinefulfillment(df_machines, batchID):
	df_machines['Machine'] = df_machines['Machine'].astype(int)
	df_machines.to_excel('df_machine.xlsx')
	N = len(df_machines['Machine'])
	x = np.arange(N)
	df_machines['BatchID'] = batchID
	engine = db_engine()
	df_machines = df_machines[['BatchID', 'Machine', 'Time Full total', 'Time Empty total']]
	df_machines = df_machines.rename(columns={"Time Full total": "time_full", 'Time Empty total': "time_empty"})
	df_machines.to_sql("P_fill", con=engine, if_exists='append', index=False)


""" Deprecated 13/9/2020 - Implemented in vue.js
	font = {'family': 'arial',
			'weight': '400',
			'size': 30}

	plt.rc('font', **font)
	plt.style.use('seaborn-darkgrid')
	rec1 = df_machines['Time Full total']
	rec2 = df_machines['Time Empty total']
	fig, ax = plt.subplots(figsize=(80, 20))
	width = 0.35
	rects1 = ax.bar(x, rec1, width)
	rects2 = ax.bar(x + width, rec2, width)
	ax.set_ylabel('Time in hours')
	ax.set_xlabel('Machines / Work centers')
	ax.set_title('Machine fulfillment')
	ax.set_xticks(x + width / 2)
	ax.set_xticklabels(df_machines['Machine'])
	start, end = ax.get_ylim()
	stepsize = end / 20
	ax.yaxis.set_ticks(np.arange(start, end, stepsize))
	ax.legend((rects1[0], rects2[0]), ('Time Full', 'Time Empty'), prop={'size': 30})
	#plt.draw()
	plt.figure(figsize=(20, 10))
	plt.savefig("fulfillment.png", dpi=150)
	plt.show()
 """


def plannedchart(df, batchID):
	labels = 'Planned', 'Not planned', 'Unknown'
	connection = db_connection()
	cursor = connection.cursor()
	cursor.execute("SELECT COUNT(batchID) FROM dbo.Orders WHERE batchID = ?", batchID)
	(amount,) = cursor.fetchone()
	planned = df['Amount of Orders Planned'][0]
	nplanned = df['Amount of Orders Not Planned'][0]
	"""print(amount)
	print(planned)
	print(nplanned) """
	unknown = int(amount) - int(planned) - int(nplanned)
	connection = db_connection()
	cursor = connection.cursor()
	cursor.execute('INSERT INTO dbo.P_planned(BatchID, planned, not_planned, unknown) VALUES(?,?,?,?)', batchID,
				   planned, nplanned, unknown)
	connection.commit()
	""" Deprecated 13/9/2020 - Implemented in vue.js
	sizes = [planned, nplanned, unknown]
	explode = (0, 0.1, 0)
	plt.style.use('seaborn-darkgrid')
	font = {'family': 'arial',
			'weight': '400',
			'size': 14}

	plt.rc('font', **font)

	fig1, ax1 = plt.subplots()

	ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
			shadow=True, startangle=90)
	ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
	ax1.set_title('Orders planned')
	plt.savefig("planned.jpg", dpi=150) """


def totalfulfillment(total_f_p, total_e_p, batchID):
	connection = db_connection()
	cursor = connection.cursor()
	cursor.execute('INSERT INTO dbo.P_filltotal(BatchID, total_filled, total_empty) VALUES(?,?,?)', batchID, total_f_p,
				   total_e_p)
	connection.commit()
	labels = 'Filled', 'Empty'
	""" Deprecated 13/9/2020 - Implemented in vue.js
	sizes = [total_f_p, total_e_p]
	plt.style.use('seaborn-darkgrid')
	font = {'family': 'arial',
			'weight': '400',
			'size': 14}

	plt.rc('font', **font)

	fig1, ax1 = plt.subplots()

	ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
			shadow=True, startangle=90)
	ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
	ax1.set_title('Total Machine fulfillment')
	plt.savefig("total_fulfillment.jpg", dpi=150)
 """


def conflicts(df, batchID):
	x = np.arange(3)
	overtime = df['Amount of orders planned overtime'][0]
	conflict = df['Conflicted Mould changes'][0]
	notindb = df['Amount of Item numbers not in the Database'][0]
	connection = db_connection()
	cursor = connection.cursor()
	cursor.execute('INSERT INTO dbo.P_conflict(BatchID, overtime, conflict ,notindb) VALUES(?,?,?,?)', batchID,
				   overtime, conflict, notindb)
	connection.commit()
	""" Deprecated 13/9/2020 - Implemented in vue.js
	y = [overtime, conflict, notindb]
	fig, ax = plt.subplots()
	plt.bar(x, y)
	plt.style.use('seaborn-darkgrid')
	conflict = df['Conflicted Mould changes'][:0]
	notindb = df['Amount of Item numbers not in the Database'][:0]
	plt.xticks(x, ('Overtime', 'Mould conflicts', 'Unknown items'))
	plt.title("General conflicts")
	plt.ylabel("Number of cases")
	plt.savefig("conflicts.jpg", dpi=150) """
