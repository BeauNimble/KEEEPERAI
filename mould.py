# Importeren van packages
import pandas as pd
import datetime
from timey import new_time


def mouldChangeCapacity(start_date, end_date):
    mould_change_capacity = pd.read_excel('Mould change capacity.xlsx',
                                          header=0)  # The amount of mould changes that can be done
    mould_change_capacity['Date'] = mould_change_capacity['Date'].astype(str)
    mould_change_capacity = mould_change_capacity.set_index('Date')
    morning_shift = mould_change_capacity['Morning shift']  # The capacity of the morning shift
    afternoon_shift = mould_change_capacity['Afternoon shift']  # The capacity of the afternoon shift
    night_shift = mould_change_capacity['Night shift']  # The capacity of the night shift
    change_capacity = {}  # The dict containing all the mould change capacities of every planning day
    date = start_date  # The date that the planning starts
    while date <= end_date:
        if str(date) in mould_change_capacity.index:  # Fill up with the given capacity for each date
            change_capacity[str(date)] = [morning_shift[str(date)], afternoon_shift[str(date)], night_shift[str(date)]]
        else:
            change_capacity[str(date)] = [4, 3, 3]  # Fills up the (standard) capacity for each date
        date = new_time(date.year, date.month, date.day+1)
    return change_capacity


def mouldChange(begin_time, end_time, mc, machine, order, mould, when):  # Adds the mould change to the list
    """
    :param begin_time: The time that the mould change starts
    :param end_time: The time that the mould change ends
    :param mc: The list containing all the mould changes
    :param machine: The machine that the mould is changed on
    :param order: The order that the mould is changed for
    :param mould: The mould that is changed
    :param when: In what stadium the mould change happens
    :return: Adds the mould change to the list
    """
    mc.append([machine, begin_time, end_time, mould, when, order])


def problems(mould):  # Calculates the amount of problems in mould changes and returns them
    """
    :param mould: The list containing the mould changes (time)
    :return: The number of problems and a list containing the problematic mould changes (mould and time)
    """
    problem = 0  # The amount of problems between mould changes.
    when = []
    for i in range(len(mould)):
        change = 0  # The number of mould changes at the current time
        for j in range(i, len(mould)):
            if mould[j][1] <= mould[i][1] <= mould[j][2] or mould[j][1] <= mould[i][2] <= mould[j][2]:
                change += 1
        if change > 1:  # If there is more than 1 mould change at a time
            problem += 1
            when.append([mould[i][0], mould[i][1].strftime("%d %X"), mould[i][2].strftime("%d %X")])
    return problem, when


def problem(begin, end, changes):  # Tests whether there is a problem to have a new mould change
    """
    :param begin: The time the new mould change starts
    :param end: The time the new mould change ends
    :param changes: The list containing the mould changes planned untill now
    :return: True or False depending on whether or not there has already been planned a mould change
    """
    for i in range(len(changes)):
        if changes[i][1] <= begin <= changes[i][2] or changes[i][1] <= end <= changes[i][2]:
            return True  # There is already a mould changes scheduled during this time
    return False  # There is no problem planning the new mould change


def printMould(changes, track):  # Prints the mould changes schedule as a dataframe
    """
    :param changes: The list of all mould changes with their machines included
    :param track: Keeps track of information, like the number of (conflicting) mould changes
    :return: Adds the schedule of all mould changes to an excel file and retuns the amount of changes
    """
    print(str(len(changes)) + " mould changes scheduled")
    p, prob = problems(changes)
    track.conflicted_mould = p  # The number of conflicted mould changes
    if p > 0:
        print(str(p) + " mould schedule problems")
    else:
        print("No problems planning moulds")

    sched = pd.DataFrame(columns=['Order', 'Work ctr', 'Mould', 'Start Time', 'Finish Time', 'When'])  # Makes the dataframe for the output
    for i in range(len(changes)):
        new_row = {'Order': changes[i][5],
                   'Work ctr': changes[i][0],
                   'Mould': changes[i][3],
                   'Start Time': changes[i][1].strftime("%d %X"),
                   'Finish Time': changes[i][2].strftime("%d %X"),
                   'When': changes[i][4]}
        sched = sched.append(new_row, ignore_index=True)  # adds new row to the dataframe under the existing rows
    sched = sched.sort_values(by=['Start Time', 'Finish Time'])
    sched.to_excel("Mould-planning.xlsx")
    return len(changes)


def getMouldMachineTime(quantity, mould, work_center, mld):
    """
    :param quantity: The quantity of the order
    :param mould: The mould of the order
    :param work_center: The work center to plan the order on
    :param mld: A dataframe containing the cycle times for items and moulds on certain work centers
    :return: If possible an estimation of the duration to make a certain quantity of a dummy product
    """
    if str(mould) != 'nan' and str(mould) != 'None':  # If a mould is given, with which the cycle time can be estimated
        md = mld.loc[mld['Mould'] == mould]  # Only look at the part of the dataframe with the correct mould
        number_w = 0  # The number of work centers to get the average cycle time from
        cycle_time_cbnd = float(0)  # The combined average cycle time of all those work centers
        for m in md.index:  # For each instance of the mould
            if md['Mean zcs'][m] != 'nan' and md['Mean zcs'][m] != 'None':
                number_w += 1
                cycle_time_cbnd += md['Mean zcs'][m].astype(float)
        if number_w > 0:  # If there were instances of the mould being used in the data
            cycle_time = cycle_time_cbnd / number_w
            time = cycle_time * quantity
            return datetime.datetime.fromtimestamp(time)
    if str(work_center) != 'nan' and str(work_center) != 'None':  # If the machine is given
        mchn = pd.read_excel("avg_machine_times.xlsx", header=0)
        mchn['Work center'] = mchn['Work center'].astype(str)
        mchn = mchn.set_index('Work center')
        if work_center in mchn.index:
            cycle_time = mchn.loc[work_center, 'total']  # Get the cycle time from the work center
            time = cycle_time * quantity
            return datetime.datetime.fromtimestamp(time)
    return False


def ChangeMould(work_center_id, product, old_mould, m_date, mould_changes, mould_change_capacity):
    """
    :param work_center_id: The number of the work center that the mould is changed on
    :param product: The product that is the reason the mould needs to be changed
    :param old_mould: The mould that was previously on the work center
    :param m_date: The current date and time of the planning of the work center
    :param mould_changes: The list containg all the mould changes that have already happened
    :param mould_change_capacity: A list containing the number of mould changes that can still happen for each shift each day
    :return: The mould change is scheduled and the date and time that the mould change ends is returned
    """

    """
    Get a possible datetime for the mould change to start 
    when there is still capacity left
    """
    begin = m_date  # The date and time that the mould change begins
    change = False  # True if the mould change can happen at this time according to the capacity
    while not change:
        if not str(datetime.datetime(begin.year, begin.month, begin.day)) in mould_change_capacity.keys():  # The mould change tries to be scheduled outside the allowed date range
            return False
        if 6 <= begin.hour <= 14:  # 6am - 2pm
            if mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][0] > 0:  # The capacity allows a mould change
                mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][0] -= 1  # The capacity gets decreased
                change = True  # The mould change will get planned
            else:
                begin = new_time(begin.year, begin.month, begin.day, begin.hour+1, begin.minute, begin.second)
        elif 14 <= begin.hour <= 22:  # 2pm - 10pm
            if mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][1] > 0:  # The capacity allows a mould change
                mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][1] -= 1
                change = True
            else:
                begin = new_time(begin.year, begin.month, begin.day, begin.hour+1, begin.minute, begin.second)
        elif 22 <= begin.hour <= 24 or 0 <= begin.hour <= 6:  # 10 pm - 6am
            if mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][2] > 0:  # The capacity allows a mould change
                mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][2] -= 1
                change = True
            else:
                begin = new_time(begin.year, begin.month, begin.day, begin.hour+1, begin.minute, begin.second)

    """
    Get the time that the changes of the old and new mould are done
    """
    add_time = getMouldChangeTime(product.moulds, type_when="in")
    add_time += getMouldChangeTime(old_mould, type_when="out")
    finished = new_time(begin.year, begin.month, begin.day, begin.hour, begin.minute + add_time, begin.second)

    """
    Find whether there are problems scheduling the mould change
    If there is a problem, the change will be tried to be scheduled 10 min later
    """
    if not problem(begin, finished, mould_changes):  # There isn't a conflict while changing the mould (only 1 change at a time)
        mouldChange(begin, finished, mould_changes, work_center_id, product.order, product.moulds, "Change between moulds")
    else:  # There is a conflict while changing the mould at this time
        if 6 <= begin.hour <= 14:  # 6am - 2pm
            mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][0] += 1  # Turn back the change in capacity
        elif 14 <= begin.hour <= 22:  # 2pm - 10pm
            mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][1] += 1
        elif 22 <= begin.hour <= 24 or 0 <= begin.hour <= 6:  # 10 pm - 6am
            mould_change_capacity[str(datetime.datetime(begin.year, begin.month, begin.day))][2] += 1
        m_date = new_time(begin.year, begin.month, begin.day, begin.hour, begin.minute + 10, begin.second)
        return ChangeMould(work_center_id, product, old_mould, m_date, mould_changes, mould_change_capacity)  # Plan the mould change again
    return finished  # There is no conflict, so return the time when the mould change is done


def getInsertChangeTime(mould):
    insert_complexity = pd.read_excel('Insert changes times per mould.xlsx', header=0)
    insert_complexity = insert_complexity[
        ['Number of mould', 'Change on machine', 'In mould shop (easy)', 'In mould shop (complex)']]
    insert_complexity = insert_complexity.set_index('Number of mould')  # The complexity of insert changes
    insert_complexity_time = pd.read_excel('Insert changes times per mould.xlsx', usecols="I:K", header=0)

    if mould in insert_complexity.index:
        change1 = str(insert_complexity.loc[mould, 'Change on machine'])
        change2 = str(insert_complexity.loc[mould, 'In mould shop (easy)'])
        change3 = str(insert_complexity.loc[mould, 'In mould shop (complex)'])

        if change1 != 'nan' and change1 != 'None':
            return int(insert_complexity_time['Change on machine.1'][
                           1])  # .1 because there are 2 columns with the same header name
        if change2 != 'nan' and change2 != 'None':
            return int(insert_complexity_time['In mould shop (easy).1'][1])
        if change3 != 'nan' and change3 != 'None':
            return int(insert_complexity_time['In mould shop (complex).1'][1])

    return int(insert_complexity_time['In mould shop (easy).1'][1])


def getMouldChangeTime(mould, type_when="both"):
    df_mould = sizeMould()  # A dataframe containing the complexity of the mould changes
    complexity = df_mould['complexity']
    if type_when == "both":
        switch = {
            "laborious": int(df_mould["laborious"][2]),
            "medium": int(df_mould["medium"][2]),
            "easy": int(df_mould["easy"][2])
        }
    elif type_when == "in":
        switch = {
            "laborious": int(df_mould["laborious"][1]),
            "medium": int(df_mould["medium"][1]),
            "easy": int(df_mould["easy"][1])
        }
    elif type_when == "out":
        switch = {
            "laborious": int(df_mould["laborious"][0]),
            "medium": int(df_mould["medium"][0]),
            "easy": int(df_mould["easy"][0])
        }

    if not str(mould) in df_mould.index:  # The mould has no changeover time data
        time = switch.get("medium", "Invalid size")  # Use average time
    else:
        size = complexity[str(mould)]  # get complexity of mould change
        if not type(size) is str:  # if mould is in dataframe more than once
            try:
                size = size[0]
            except:
                size = "medium"
        time = switch.get(size, "Invalid size")
    return time


def sizeMould():
	"""
	:return: The dataframe containing the moulds and their complexity
	"""
	df_mould = pd.read_excel('mold complexity-co time.xlsx', header=0)
	df_mould = df_mould.set_index('Number of mould')  # Sets the mould as the index of the dataframe
	return df_mould
