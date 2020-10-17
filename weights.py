# Importeren van packages
import pandas as pd
from POC import update, db_connection, db_engine
from planning import printNotScheduled, printSchedule
from read import zcs, MachineDf, AddInsert
from classes import TrackingData, Product, Machine
from ordering import addAndSortMachines
from mould import getMouldMachineTime, mouldChangeCapacity, printMould
from timey import get_half_time, new_time, combineOrder, withinTime
from performance import addMachineTrackingInfo, trackDataFrame


def planningWeight(df, start_date, end_date, batchID):
    """
    :param df: The dataframe containing the input
    :param start_date: The date that the planning starts
    :param end_date: The date the planning ends (exclusive, so before)
    :param batchID:
    :return: Makes the planning of the input
    """

    """ Get the weight of combining orders with the same mould """
    connection = db_connection()
    cursor = connection.cursor()
    cursor.execute('SELECT Weight_4 FROM dbo.Batch WHERE ID = (?)', batchID)
    (weight_mould,) = cursor.fetchone()  # This code gets the weights given to combining orders with the same mould

    """
    Initiate tracking the data,
    give a priority to the orders,
    and get all available work centers
    """
    track = TrackingData()  # Makes an object to track data about the planning
    df_zcs = zcs()  # Obtain the dataframe containing info about the materials and the work centers they can be made on
    track.orders_planned = len(df['Order'])  # Track the amount of orders that has to be planned
    df['Estimated time'] = pd.to_datetime(df['Estimated time'],
                                          unit='s')  # Makes the estimated duration to produce a product a datetime object
    df['needed on'] = pd.to_datetime(df['needed on'], infer_datetime_format=True)  # Sets the needed on date to datetime
    df, wrong = prioritiseOrdersDf(df, df_zcs, start_date, end_date,
                                   batchID)  # Wrong is the list of orders that couldn't be planned (not enough information)
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
    later = {}
    dummy_amount = 0
    for i in range(len(df["material_number"])):  # Make each order and add it to one of the dicts
        # Making a dummy order
        if str(material_number[i]) == 'nan' or str(
                material_number[i]) == 'None':  # THe order has no material number --> dummy product
            dummy_amount += 1
            machine = [str(work_center[i]).replace('.0', '')]  # Get the work center from the input
            pt = getMouldMachineTime(quantity[i], str(mould_df[i]), machine[0],
                                     df_zcs['Mould', 'Mean zcs'].sort_values(
                                         'Mould'))  # The time it takes to make the order
            if not pt:  # The duration to make the order can't be estimated
                not_planned.append([str(material_number[i]), "Dummy doesn't have enough information to calculate the duration", str(order[i])])
            else:
                insert = AddInsert(str(order[i]), df[['Order', 'Insert 1 (mould)', 'Insert 2 (mould)', 'Insert 3 (mould)', 'Insert 4 (mould)', 'Insert 5 (mould)']])  # Get the list of inserts from the input
                product = Product(str(order[i]), str(orig_document[i]),
                                  "Dummy" + str(dummy_amount), pt, "dummy product", quantity[i],
                                  str(mould_df[i]), insert, machine, n_on[i],
                                  in_production[i], prio[i])  # Make the object for the order
                if in_production[i] == 1:  # The order is already being produced on a work center
                    in_machine[product.moulds] = product
                else:  # The order is added to the list of orders that need to be planned
                    if product.moulds in in_machine.keys() and (n_on[i] <= end_date) and in_machine[product.moulds].priority + weight_mould >= product.priority:  # IF the mould is already used on a machine
                        combineOrder(in_machine[product.moulds], product)
                    elif product.moulds in products.keys() and products[product.moulds].priority + weight_mould >= product.priority:  # If the mould is already in the dict to be planned, they are combined
                        combineOrder(products[product.moulds], product)
                    elif not product.moulds in products.keys():  # If the product hasn't been ordered before
                        products[product.moulds] = product  # Add the product to the list
                    elif product.moulds in later.keys():  # If the mould is already in the dict to be planned, they are combined
                        combineOrder(later[product.moulds], product)
                    else:
                        later[product.moulds] = product

        # Making an order that is already being produced
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

        # Making a normal order
        elif type(est_time[i].hour) == int:  # The order still needs to be planned
            machine = addAndSortMachines(df_zcs, str(material_number[i]))  # Get a sorted list containing the work centers
            insert = AddInsert(str(order[i]), df[['Order', 'Insert 1 (mould)', 'Insert 2 (mould)', 'Insert 3 (mould)', 'Insert 4 (mould)', 'Insert 5 (mould)']])  # Get the inserts from the input
            product = Product(str(order[i]), str(orig_document[i]),
                              str(material_number[i]), est_time[i],
                              str(df['Material Description', i]), df['Rec./reqd quantity', i],
                              str(mould_df[i]), insert, machine, n_on[i], in_production[i], df['Priority', i])
            if product.moulds in in_machine.keys() and (n_on[i] <= end_date) and in_machine[product.moulds].priority + weight_mould >= product.priority:  # IF the mould is already used on a machine
                combineOrder(in_machine[product.moulds], product)
            elif product.moulds in products.keys() and products[product.moulds].priority + weight_mould >= product.priority:  # If the mould is already in the dict to be planned, they are combined
                combineOrder(products[product.moulds], product)
            elif not product.moulds in products.keys():  # If the product hasn't been ordered before
                products[product.moulds] = product  # Add the product to the list
            elif product.moulds in later.keys():  # If the mould is already in the dict to be planned, they are combined
                combineOrder(later[product.moulds], product)
            else:
                later[product.moulds] = product
        else:  # If the duration isn't a datetime object
            not_planned.append([str(material_number[i]), 'No time available', str(order[i])])

    """ Dynamically get the mould change capacity """
    mould_changes = []  # The list containing all the mould changes
    change_capacity = mouldChangeCapacity(start_date, end_date)

    """ Plan the orders that are already being produced first """
    for order in in_machine:  # For each order that is already being produced
        if in_machine[order].machines != 'nan' and in_machine[order].machines != 'None':  # The order has a work center
            machines[in_machine[order].machines].first(in_machine[order])  # Plan the order on the work center

    """ Add the normal of the orders to the work centers, so they can be planned """
    half_time = get_half_time(end_date, start_date)  # Calculate when half the time has passed
    for product in products:  # Plan the products in the work centers
        if products[product].machines != '[]' and products[product].machines != 'nan':  # The order has work centers to plan on
            emptiest = []  # Keeps track of the work center with the least time planned yet
            emptiest_in_time = []  # Keeps track of the work center with the least time, where the order can be planned in time
            for j in products[product].machines:  # Try each possible work center
                if j in machines.keys():  # If the work center is available
                    if not products[product].scheduled and machines[j].remainder <= end_date:  # If the order isn't planned yet and there is still time on the work center
                        if products[product].finish_date < end_date and withinTime(machines[j].remainder, products[product].time, end_date):
                            if half_time >= machines[j].remainder:
                                machines[j].add(products[product])  # Add to order to be scheduled in the work center
                            elif (emptiest_in_time == [] or emptiest_in_time[1] > machines[j].remainder) and withinTime(
                                    machines[j].remainder, products[product].time, products[product].finish_date):
                                # maybe change so it looks at a later needed on date, instead of the first of the combined orders
                                emptiest_in_time = [machines[j], machines[j].remainder]
                        if (emptiest == [] or emptiest[1] > machines[j].remainder) and machines[
                            j].remainder < end_date:
                            emptiest = [machines[j], machines[
                                j].remainder]  # The order couldn't be scheduled in time, but this is the best possible work center untill now

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
                    not_planned.append(
                        [products[product].id, "No available work center in time", products[product].order])
                    for p in products[product].combined:
                        not_planned.append([p.id, "No available work center in time", p.order])
        else:  # There was no work center to schedule the order on
            not_planned.append([products[product].id, "No work center for this item", products[product].order])
            for p in products[product].combined:
                not_planned.append([p.id, "No work center for this item", p.order])

    """ The later orders are added to workcenters """
    for product in later:  # Plan the products in the work centers
        if later[product].machines != '[]' and later[
            product].machines != 'nan':  # The order has work centers to plan on
            emptiest = []  # Keeps track of the work center with the least time planned yet
            emptiest_in_time = []  # Keeps track of the work center with the least time, where the order can be planned in time
            for j in later[product].machines:  # Try each possible work center
                if j in machines.keys():  # If the work center is available
                    if not later[product].scheduled and machines[
                        j].remainder <= end_date:  # If the order isn't planned yet and there is still time on the work center
                        if later[product].finish_date < end_date and withinTime(machines[j].remainder,
                                                                                later[product].time, end_date):
                            if half_time >= machines[j].remainder:
                                machines[j].add(later[product])  # Add to order to be scheduled in the work center
                            elif (emptiest_in_time == [] or emptiest_in_time[1] > machines[j].remainder) and withinTime(
                                    machines[j].remainder, later[product].time, later[product].finish_date):
                                # maybe change so it looks at a later needed on date, instead of the first of the combined orders
                                emptiest_in_time = [machines[j], machines[j].remainder]
                        if (emptiest == [] or emptiest[1] > machines[j].remainder) and machines[j].remainder < end_date:
                            emptiest = [machines[j], machines[
                                j].remainder]  # The order couldn't be scheduled in time, but this is the best possible work center untill now

            """
            The orders that could not be added to a work center yet
            will be added to emptiest work center, if possible
            """
            if not later[product].scheduled:  # If the order hasn't been scheduled yet
                if emptiest_in_time:
                    emptiest_in_time[0].add(later[product])
                elif emptiest:  # If there was a work center where (part of) the order could be scheduled before the end date is reached
                    emptiest[0].add(later[product])
                else:  # There was no possibility to schedule the order
                    not_planned.append([later[product].id, "No available work center in time", later[product].order])
                    for p in later[product].combined:
                        not_planned.append([p.id, "No available work center in time", p.order])
        else:  # There was no work center to schedule the order on
            not_planned.append([later[product].id, "No work center for this item", later[product].order])
            for p in later[product].combined:
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


def sortOrdersDf(df):
    """
    :param df: A dataframe with orders and the things determining their priority
    :return: The dataframe sorted based on Priority and needed on date
    """
    df = df.sort_values(by=['Priority', 'needed on', 'Amount'])
    # Sorts the dataframe in ascending order based on the needed on date and priority earlier established
    prio = df['Priority']
    df['Index'] = range(len(prio))  # Makes a new index. The other one is jumbled by the sorting
    df = df.set_index('Index')  # (Re)sets the index
    return df


def prioritiseOrdersDf(df, zcs, begin_date, end_date, batchID):
    """
    :param df: The dataframe containing the orders
    :param zcs: The material info containg the number of machines for each item number
    :param begin_date: The date that the planning begins
    :param end_date: The date that the planning end
    :return: Sorts the dataframe of the orders based on priority
    """

    """ Get the different weights """
    connection = db_connection()
    cursor = connection.cursor()  # This code gets the weights given to different aspects of the orders
    cursor.execute('SELECT Weight_1 FROM dbo.Batch WHERE ID = (?)', batchID)
    (weight_date,) = cursor.fetchone()  # The importance of planning before the needed on date
    cursor.execute('SELECT Weight_2 FROM dbo.Batch WHERE ID = (?)', batchID)
    (weight_promo,) = cursor.fetchone()  # The importance of planing a promo order
    cursor.execute('SELECT Weight_3 FROM dbo.Batch WHERE ID = (?)', batchID)
    (weight_listing,) = cursor.fetchone()  # The importance of plannig a listing order

    """ Get the cutoff points and the dates of these points """
    cursor.execute('SELECT Cutoff_promo FROM dbo.Batch WHERE ID = (?)', batchID)
    (days_over_end_date_promo,) = cursor.fetchone()# Promo orders x days over the end date can still be planned
    if days_over_end_date_promo == None:
        days_over_end_date_promo = 0
    cursor.execute('SELECT Cutoff_listing FROM dbo.Batch WHERE ID = (?)', batchID)
    (days_over_end_date_listing,) = cursor.fetchone()# Listing orders x days over the end date can still be planned
    if days_over_end_date_listing == None:
        days_over_end_date_listing = 0
    end_date_promo = new_time(end_date.year, end_date.month, end_date.day + days_over_end_date_promo)
    end_date_listing = new_time(end_date.year, end_date.month, end_date.day + days_over_end_date_listing)

    """ 
    Check whether the orders are valid
    If there is a problem the order is removed
    Otherwise it gets a weight based on different criteria
    """
    n_on = df['needed on']
    orig_document = df['Originating document']
    material_number = df['Material']
    work_center = df['Work center']
    order = df['Order']
    in_production = df['Start Point']
    mould_df = df['Mould']
    prio = []  # A list containing the priority of each order
    amoun = []  # A list containing the amount of machines each order is compatible with
    wrong = []  # A list containing the orders that aren't planned and the reason why
    for i in range(len(df["Material"])):  # For each order
        # The order is not needed before the cutoff point of the planning
        if (n_on[i] != "nan" and n_on[i] != "None") and (not (str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None')) and n_on[i] > end_date_promo:  # promo with needed on date after allowed promo needed on date
            df = df.drop([i])  # Removes the order (row)
            wrong.append([str(material_number[i]), "Needed on date after planning", str(order[i])])
        elif (n_on[i] != "nan" and n_on[i] != "None") and str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None' and n_on[i] > end_date_listing:  # listing with needed on date after allowed listing needed on date
            df = df.drop([i])  # Removes the order (row)
            wrong.append([str(material_number[i]), "Needed on date after planning", str(order[i])])

        # The order is a dummy order
        elif str(material_number[i]) == 'nan' or str(material_number[i]) == 'None':  # If there is no item number --> orders is dummy order
            if str(work_center[i]) != 'nan' and str(work_center[i]) != 'None':  # There has to be a machine given to plan dummy orders
                p = 0  # The priority of the order (lower number is planned/prioritized earlier)
                if n_on[i] == "nan" or n_on[i] == "None":
                    p += (weight_promo + weight_date + weight_listing)
                elif n_on[i] <= begin_date:
                    p -= weight_date
                elif n_on[i] <= end_date:
                    p -= (weight_date / 2)
                else:
                    p += weight_date
                if not (str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None'):  # If it is a promo order
                    p -= weight_promo  # The priority is higher
                if str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None':  # If it is a listing order
                    p -= weight_listing
                if str(in_production[i]) == str(1):  # If the order is already on a machine
                    p -= (weight_promo + weight_date + weight_listing)  # This is most important
                prio.append(p)
                amoun.append(1)  # There is only one machine that the order can be planned on
            else:
                df = df.drop([i])  # Removes the order (row)
                wrong.append([str(material_number[i]), "Dummy order has no Work center in input", str(order[i])])

        # The order is not a dummy order, but the item number isn't in the database
        elif not str(material_number[i]) in zcs.index:
            df = df.drop([i])  # Removes the order (row)
            if mould_df[i] == None or mould_df[i] == 'nan':
                wrong.append([str(material_number[i]), "Assembly order", str(order[i])])
            else:
                wrong.append([str(material_number[i]), "Item unknown - Material/Mould not known to bot", str(order[i])])

        # The order is a normal order that is in the database
        else:
            """
            If the order has no mould in the input,
            we add one from the zcs04 data or the historical data
            Otherwise the order will be removed
            """
            if str(mould_df[i]) == 'nan' or str(mould_df[i]) == 'None':  # No mould is given
                mould = zcs['Mould'].loc[str(material_number[i])]
                mould_df[i] = mould  # Adds the mould from the zcs part of the material_info dataframe
            if str(mould_df[i]) == "nan" or str(mould_df[i]) == 'None':  # If there still isn't a mould
                try:
                    mould = zcs["('Mould', 'unique')"].loc[str(material_number[i])]
                    mould = mould.replace('[', '')
                    mould = mould.replace(']', '')
                    mould = mould.split(', ')
                    mould_df[i] = mould  # Adds the mould from the historical data
                except:
                    wrong.append([str(material_number[i]), "Item unknown - No mould", str(order[i])])
            if str(mould_df[i]) == "nan" or str(mould_df[i]) == 'None':  # If the earlier data couldn't give a mould
                df = df.drop([i])  # Removes the order (row)
                wrong.append([str(material_number[i]), "Order has no mould in the input or database", str(order[i])])
            else:  # There is a mould for this order
                p = 0  # The priority of the order (lower number is planned/prioritized earlier)
                if not (str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None'):  # If it is a promo order
                    p = 1  # The priority is higher
                if str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None':  # If it is a listing order
                    p = 2
                if str(in_production[i]) == str(1):  # If the order is already on a machine
                    p = 0  # This is most important
                prio.append(p)
                amoun.append(zcs['Amount'][str(material_number[i])])

    df['Priority'] = prio  # Adds the priority of each order to the database
    df['Amount'] = amoun  # Adds the amount of compatible machines to each order
    return sortOrdersDf(df), wrong
