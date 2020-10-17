# Importeren van packages
import pandas as pd
import numpy as np
import sqlalchemy
import datetime
from POC import update, db_connection, db_engine
from read import zcs, MachineDf, AddInsert
from classes import TrackingData, Product, Machine
from ordering import prioritiseOrdersDefault, addAndSortMachines
from mould import getMouldMachineTime, mouldChangeCapacity, printMould
from timey import get_half_time, new_time, combineOrder, withinTime
from performance import addMachineTrackingInfo, trackDataFrame


# v1.0
def planning(df, start_date, end_date, batchID):
    """
    :param df: The dataframe containing the input
    :param start_date: The date that the planning starts
    :param end_date: The date the planning ends (exclusive, so before)
    :param batchID:
    :return: Makes the planning of the input
    """

    """
    Initiate tracking the data,
    give a priority to the orders,
    and get all availible work centers
    """
    track = TrackingData()  # Makes an object to track data about the planning
    df_zcs = zcs()  # Obtain the dataframe containing info about the materials and the work centers they can be made on
    track.orders_planned = len(df['Order'])  # Track the amount of orders that has to be planned
    df['Estimated time'] = pd.to_datetime(df['Estimated time'], unit='s')  # Makes the estimated duration to produce a product a datetime object
    df['needed on'] = pd.to_datetime(df['needed on'], infer_datetime_format=True)  # Sets the needed on date to datetime
    df, wrong = prioritiseOrdersDefault(df, df_zcs, start_date, end_date, batchID)  # Wrong is the list of orders that couldn't be planned (not enough information)
    machines = MachineDf(start_date, track)  # Makes a dictionary containing all available machines

    """
    Get the dictionaries to plan the order
    and then create the objects for the order and put those in the dicts
    """
    n_on = df['needed on']
    orig_document = df['Originating document']
    material_number = df['Material']
    work_center = df['Work center']
    order = df['Order']
    in_production = df['Start Point']
    mould_df = df['Mould']
    quantity = df['Rec./reqd quantity']
    prio = df['Priority']
    est_time = df['Estimated time']
    desc = df['Material Description']
    not_planned = []  # A list containing the orders that couldn't be planned and the reason why
    products = {}  # Dict of orders to assign to machines
    in_machine = {}  # Dict of orders already being processed in machines
    dummy_amount = 0
    for i in range(len(order)):
        #  Making a dummy order
        if str(material_number[i]) == 'nan' or str(
                material_number[i]) == 'None':  # THe order has no material number --> dummy product
            dummy_amount += 1
            machine = [str(work_center[i]).replace('.0', '')]  # Get the work center from the input
            pt = getMouldMachineTime(quantity[i], str(mould_df[i]), machine[0],
                                     df_zcs['Mould', 'Mean zcs'].sort_values('Mould'))  # The time it takes to make the order
            if not pt:  # The duration to make the order can't be estimated
                not_planned.append(
                    [str(material_number[i]), "Dummy doesn't have enough information to calculate the duration",
                     str(order[i])])
            else:
                insert = AddInsert(str(order[i]), df[['Order', 'Insert 1 (mould)', 'Insert 2 (mould)', 'Insert 3 (mould)', 'Insert 4 (mould)',
                     'Insert 5 (mould)']])  # Get the list of inserts from the input
                product = Product(str(order[i]), str(orig_document[i]),
                                  "Dummy" + str(dummy_amount), pt, "dummy product", quantity[i],
                                  str(mould_df[i]), insert, machine, n_on[i],
                                  in_production[i], prio[i])  # Make the object for the order
                if in_production[i] == 1:  # The order is already being produced on a work center
                    in_machine[product.moulds] = product
                else:  # The order is added to the list of orders that need to be planned
                    if product.moulds in in_machine.keys() and n_on[i] <= end_date:  # IF the mould is already used on a machine
                        combineOrder(in_machine[product.moulds], product)
                    elif product.moulds in products.keys():  # If the mould is already in the dict to be planned, they are combined
                        combineOrder(products[product.moulds], product)
                    else:  # If the product hasn't been ordered before
                        products[product.moulds] = product  # Add the product to the list

        #  Making an order that is already being produced
        elif in_production[i] == 1 and type(est_time[i].hour) == int:
            machine = str(work_center[i]).replace('.0', '')  # Get the work center from the input
            if str(machine) == 'nan':
                not_planned.append(
                    [str(material_number[i]), 'No work center for a product that is already in production',
                     str(order[i])])
            else:
                insert = AddInsert(str(order[i]), df[['Order', 'Insert 1 (mould)', 'Insert 2 (mould)', 'Insert 3 (mould)', 'Insert 4 (mould)', 'Insert 5 (mould)']])  # Get the inserts from the input
                product = Product(str(order[i]), str(orig_document[i]),
                                  str(material_number[i]), est_time[i],
                                  str(desc[i]), quantity[i],
                                  str(mould_df[i]), insert,
                                  machine, n_on[i], in_production[i],
                                  prio[i])
                in_machine[
                    product.moulds] = product  # The order is added to the list of orders that are already being produced on a work center

        #  Making a normal order
        elif type(est_time[i].hour) == int:
            machine = addAndSortMachines(df_zcs, str(material_number[i]))  # Get a sorted list containing the work centers
            insert = AddInsert(str(order[i]), df[
                ['Order', 'Insert 1 (mould)', 'Insert 2 (mould)', 'Insert 3 (mould)', 'Insert 4 (mould)',
                 'Insert 5 (mould)']])  # Get the inserts from the input
            product = Product(str(order[i]), str(orig_document[i]),
                              str(material_number[i]), est_time[i],
                              str(desc[i]), quantity[i],
                              str(mould_df[i]), insert,
                              machine, n_on[i], in_production[i], prio[i])
            if product.moulds in in_machine.keys() and n_on[i] <= end_date:  # IF the mould is already used on a machine
                combineOrder(in_machine[product.moulds], product)
            elif product.moulds in products.keys():  # If the mould is already in the dict to be planned, they are combined
                combineOrder(products[product.moulds], product)
            else:  # If the product hasn't been ordered before
                products[product.moulds] = product  # Add the product to the list
        else:  # If the duration isn't a datetime object
            not_planned.append([str(material_number[i]), 'No time available', str(order[i])])

    """ Dynamically get the mould change capacity """
    mould_changes = []  # The list containing all the mould changes
    change_capacity = mouldChangeCapacity(start_date, end_date)

    """ Plan the orders that are already being produced first """
    for order in in_machine:  # For each order that is already being produced
        if in_machine[order].machines != 'nan' and in_machine[order].machines != 'None':  # The order has a work center
            machines[in_machine[order].machines].first(in_machine[order])  # Plan the order on the work center

    """ Add the rest of the orders to the work centers, so they can be planned """
    half_time = get_half_time(end_date, start_date)  # Calculate when half the time has passed
    for product in products:  # Plan the products in the work centers
        if products[product].machines != '[]' and products[product].machines != 'nan':  # The order has work centers to plan on
            emptiest = []  # Keeps track of the work center with the least time planned yet
            emptiest_in_time = []  # Keeps track of the work center with the least time, where the order can be planned in time
            for j in products[product].machines:  # Try each possible work center
                if j in machines.keys():  # If the work center is available
                    if not products[product].scheduled and machines[
                        j].remainder <= end_date:  # If the order isn't planned yet and there is still time on the work center
                        if products[product].finish_date < end_date and withinTime(machines[j].remainder,products[product].time, end_date):
                            if half_time >= machines[j].remainder:
                                machines[j].add(products[product])  # Add to order to be scheduled in the work center
                            elif (emptiest_in_time == [] or emptiest_in_time[1] > machines[j].remainder) and withinTime(machines[j].remainder, products[product].time, products[product].finish_date):
                                # maybe change so it looks at a later needed on date, instead of the first of the combined orders
                                emptiest_in_time = [machines[j], machines[j].remainder]
                        if (emptiest == [] or emptiest[1] > machines[j].remainder) and machines[j].remainder < end_date:
                            emptiest = [machines[j], machines[j].remainder]  # The order couldn't be scheduled in time, but this is the best possible work center untill now

            """
            The orders that could not be added to a work center yet
            will be added to emptiest work center, if possible
            """
            if not products[product].scheduled:  # If the order hasn't been scheduled yet
                if emptiest_in_time:
                    emptiest_in_time[0].add(products[product])
                elif emptiest:  # If there was a work center where (part of) the order could be scheduled before the end date is reached
                    emptiest[0].add(products[product])
                else:  # There was no possibility to schedule the order
                    not_planned.append([products[product].id, "No available work center in time", products[product].order])
                    for p in products[product].combined:
                        not_planned.append([p.id, "No available work center in time", p.order])
        else:  # There was no work center to schedule the order on
            not_planned.append([products[product].id, "No work center for this item", products[product].order])
            for p in products[product].combined:
                not_planned.append([p.id, "No work center for this item", p.order])

    """
    The orders that are added to the work centers
    are first ordered and then scheduled
    """
    for m in machines:  # For each possible work center
        if len(machines[m].products) > 0:  # There is an order to plan
            machines[m].sortProducts(mould_changes, track, change_capacity, not_planned, end_date)  # Sorts the orders and plans them

    """
    Information for tracking the orders is added
    After this the schedule, mould changes, and not scheduled orders are written to the output
    """
    addMachineTrackingInfo(machines, start_date, end_date, track)  # Makes the tracking information for how much the work centers are used
    printSchedule(machines, end_date, track, batchID)
    printMould(mould_changes, track)
    printNotScheduled(not_planned, wrong, track, batchID)
    trackDataFrame(track, batchID)


def printNotScheduled(not_planned, wrong, track, batchID):
    """
    :param not_planned: The list of orders that weren't planned
    :param wrong: The list of orders that don't have enough data
    :param track: Keeps track of the amount of orders that couldn't be planned
    :param batchID:
    :return: Makes a dataframe containing the orders that weren't planned
    """
    no = pd.DataFrame(columns=['Order', 'Material', 'Why'])  # Makes the dataframe for the output
    if len(wrong) != 0:
        for w in wrong:
            track.items_not_in_database += 1
            new_row = {'Order': w[2], 'Material': w[0], 'Why': w[1]}
            no = no.append(new_row, ignore_index=True)  # Adds new row to the dataframe
    if len(not_planned) != 0:
        for n in not_planned:
            track.orders_not_planned += 1
            new_row = {'Order': n[2], 'Material': n[0], 'Why': n[1]}
            no = no.append(new_row, ignore_index=True)  # Adds a new row to the dataframe
    track.orders_planned -= (track.orders_not_planned + track.items_not_in_database)
    no = no.sort_values(by=['Material', 'Why'])

    # Operational section
    no['BatchID'] = batchID
    no = no.rename({'Order': 'OrderNo', 'Why': 'Reason'}, axis=1)
    print(no.columns)
    unknown = no[['BatchID', 'Material', 'Reason']]
    engine = db_engine()
    no.to_sql("Unknown", con=engine, if_exists='append', index=False)
    no.to_excel("Products not appended.xlsx")


def printSchedule(machines, end_date, track, batchID):  # Print de schedule in de form van een dataframe
    """
    :param machines: The list of all work centers with their schedules included
    :param end_date: Date before which the planning needs to be done
    :param track: Keeps track of how much the work centers are used
    :return: Makes a dataframe of the schedule of all work centers
    """
    sched = pd.DataFrame(
        columns=['Work ctr', 'Order', 'Originating document', 'Material', 'Description', 'Mould', 'Insert',
                 'Target qty', 'Impact of color',
                 'Start Date',
                 'Start Time', 'Finish Date', 'Finish Time', 'Needed on',
                 'Duration'])  # Makes the dataframe and columns for the output
    for i in machines:
        for schedule in machines[i].schedule:  # For each work center with an order in de schedule, add it to dataframe
            if schedule[0].originating_document == 'nan' or schedule[0].originating_document == 'None':
                schedule[0].originating_document = ''
            schedule[0].insert = str(schedule[0].insert).replace("'", '')  # Makes the list of inserts into a string
            schedule[0].insert = schedule[0].insert.replace('[',
                                                            '')  # Mark can remove this and add it as seperate output to the dataframe
            schedule[0].insert = schedule[0].insert.replace(']', '')  # The order isn't changed
            if schedule[2] > end_date:  # If the order is scheduled outside the allowed date range
                track.orders_overtime += 1
            if type(schedule[0].finish_date.day) == int:  # There is a needed on date given
                if schedule[0].duration.day == 1:  # The duration of making the product is less than a day
                    new_row = {'Work ctr': machines[i].id, 'Order': schedule[0].order,
                               'Originating document': schedule[0].originating_document, 'Material': schedule[0].id,
                               'Description': schedule[0].description, 'Mould': schedule[0].moulds,
                               'Insert': schedule[0].insert,
                               'Target qty': schedule[0].quantity, 'Impact of color': schedule[0].color_impact,
                               'Start Date': schedule[1].strftime("%d-%m-%Y"), 'Start Time': schedule[1].strftime("%X"),
                               'Finish Date': schedule[2].strftime("%d-%m-%Y"),
                               'Finish Time': schedule[2].strftime("%X"),
                               'Needed on': schedule[0].finish_date.strftime("%d-%m-%Y"),
                               'Duration': schedule[0].duration.strftime("%X"),
                               'StartPoint': schedule[0].start}
                else:  # The duration is longer than a day
                    schedule[0].duration = datetime.datetime(schedule[0].duration.year, schedule[0].duration.month,
                                                             schedule[0].duration.day - 1, schedule[0].duration.hour,
                                                             schedule[
                                                                 0].duration.minute)  # Change the duration to remove the extra day that is always there
                    new_row = {'Work ctr': machines[i].id, 'Order': schedule[0].order,
                               'Originating document': schedule[0].originating_document, 'Material': schedule[0].id,
                               'Description': schedule[0].description, 'Mould': schedule[0].moulds,
                               'Insert': schedule[0].insert,
                               'Target qty': schedule[0].quantity, 'Impact of color': schedule[0].color_impact,
                               'Start Date': schedule[1].strftime("%d-%m-%Y"), 'Start Time': schedule[1].strftime("%X"),
                               'Finish Date': schedule[2].strftime("%d-%m-%Y"),
                               'Finish Time': schedule[2].strftime("%X"),
                               'Needed on': schedule[0].finish_date.strftime("%d-%m-%Y"),
                               'Duration': schedule[0].duration.strftime("%d %X"),
                               'StartPoint': schedule[0].start}
            else:  # No needed on date is given
                if schedule[0].duration.day == 1:  # The duration of making the product is less than a day
                    new_row = {'Work ctr': machines[i].id, 'Order': schedule[0].order,
                               'Originating document': schedule[0].originating_document, 'Material': schedule[0].id,
                               'Description': schedule[0].description, 'Mould': schedule[0].moulds,
                               'Insert': schedule[0].insert,
                               'Target qty': schedule[0].quantity, 'Impact of color': schedule[0].color_impact,
                               'Start Date': schedule[1].strftime("%d-%m-%Y"), 'Start Time': schedule[1].strftime("%X"),
                               'Finish Date': schedule[2].strftime("%d-%m-%Y"),
                               'Finish Time': schedule[2].strftime("%X"),
                               'Needed on': "No needed on given",
                               'Duration': schedule[0].duration.strftime("%X"),
                               'StartPoint': schedule[0].start}
                else:  # The duration is longer than a day
                    schedule[0].duration = datetime.datetime(schedule[0].duration.year, schedule[0].duration.month,
                                                             schedule[0].duration.day - 1, schedule[0].duration.hour,
                                                             schedule[
                                                                 0].duration.minute)  # Change the duration to remove the extra day that is always there
                    new_row = {'Work ctr': machines[i].id, 'Order': schedule[0].order,
                               'Originating document': schedule[0].originating_document, 'Material': schedule[0].id,
                               'Description': schedule[0].description, 'Mould': schedule[0].moulds,
                               'Insert': schedule[0].insert,
                               'Target qty': schedule[0].quantity, 'Impact of color': schedule[0].color_impact,
                               'Start Date': schedule[1].strftime("%d-%m-%Y"), 'Start Time': schedule[1].strftime("%X"),
                               'Finish Date': schedule[2].strftime("%d-%m-%Y"),
                               'Finish Time': schedule[2].strftime("%X"),
                               'Needed on': "No needed on given",
                               'Duration': schedule[0].duration.strftime("%d %X"),
                               'StartPoint': schedule[0].start}
            sched = sched.append(new_row, ignore_index=True)  # Adds a new row to the dataframe

    # Operational section
    sched['BatchID'] = batchID
    sched['Insert 1 (mould)'] = np.nan
    sched['Insert 2 (mould)'] = np.nan
    sched['Insert 3 (mould)'] = np.nan
    sched['Insert 4 (mould)'] = np.nan
    sched['Insert 5 (mould)'] = np.nan
    # sched['StartPoint'] = np.nan
    # Assign array to insert no.
    for index, row in sched.iterrows():
        if row['StartPoint'] == 'nan' or row['StartPoint'] == 'None':
            sched.loc[index, 'StartPoint'] = 0
        curr = row['Insert']
        arr = [x.strip() for x in curr.split(',')]
        arr_len = len(arr)
        i = 0
        x = 1
        while i < arr_len:
            sched.loc[index, 'Insert ' + str(x) + ' (mould)'] = arr[i]
            i += 1
            x += 1
    sched = sched[
        ['Work ctr', 'Order', 'Material', 'Description', 'Mould', 'Target qty', 'Originating document',
         'Impact of color', 'Start Date',
         'Start Time', 'Finish Date', 'Needed on', 'Finish Time', 'Duration', 'Insert 1 (mould)', 'Insert 2 (mould)',
         'Insert 3 (mould)', 'Insert 4 (mould)', 'Insert 5 (mould)', 'StartPoint', 'BatchID']]
    print(sched.columns)
    engine = db_engine()
    sched.to_sql("Plans", con=engine, if_exists='append', index=False)

    # # Upload tracking info
    # tracking_df = pd.read_excel('TrackingInfo.xlsx')
    # tracking_df['BatchID'] = batchID
    # tracking_df = tracking_df.drop(tracking_df.columns[[0]], axis=1)
    # tracking_df.to_sql("Stats", con=engine, if_exists='append', index=False)

    # Update status
    update("Finished", batchID)
    # print(sched.to_string(index=False))
