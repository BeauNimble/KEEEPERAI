# Importeren van packages
import pandas as pd
from POC import db_connection
from timey import new_time


def sortOrdersDefault(df):
    """
    :param df: A dataframe with orders and the things determining their priority
    :return: The dataframe sorted based on Priority and needed on date
    """
    df = df.sort_values(by=['needed on', 'Priority', 'Amount'])
    # Sorts the dataframe in ascending order based on the needed on date and priority earlier established
    prio = df['Priority']
    df['Index'] = range(len(prio))  # Makes a new index. The other one is jumbled by the sorting
    df = df.set_index('Index')  # (Re)sets the index
    return df


def prioritiseOrdersDefault(df, zcs, begin_date, end_date, batchID):
    """
    :param df: The dataframe containing the orders
    :param zcs: The material info containg the number of machines for each item number
    :param begin_date: The date that the planning begins
    :param end_date: The date that the planning end
    :return: Sorts the dataframe of the orders based on priority
     """

    """ Get the cutoff points and the dates of these points """
    connection = db_connection()
    cursor = connection.cursor()
    cursor.execute('SELECT Cutoff_promo FROM dbo.Batch WHERE ID = (?)', batchID)
    (days_over_end_date_promo,) = cursor.fetchone()  # Promo orders x days over the end date can still be planned
    if days_over_end_date_promo == None:
        days_over_end_date_promo = 0
    cursor.execute('SELECT Cutoff_listing FROM dbo.Batch WHERE ID = (?)', batchID)
    (days_over_end_date_listing,) = cursor.fetchone()  # Listing orders x days over the end date can still be planned
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
    for i in range(len(order)):  # For each order
        # The order is not needed before the cutoff point of the planning
        if (n_on[i] != "nan" and n_on[i] != "None") and (not (str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None')) and n_on[i] > end_date_promo:
            # promo with needed on date after allowed promo needed on date
            df = df.drop([i])  # Removes the order (row)
            wrong.append([str(material_number[i]), "Needed on date after planning", str(order[i])])
        elif (n_on[i] != "nan" and n_on[i] != "None") and str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None' and n_on[i] > end_date_listing:
            # listing with needed on date after allowed listing needed on date
            df = df.drop([i])  # Removes the order (row)
            wrong.append([str(material_number[i]), "Needed on date after planning", str(order[i])])

        # The order is a dummy order
        elif str(material_number[i]) == 'nan' or str(material_number[i]) == 'None':  # If there is not item number (orders is dummy order)
            if str(work_center[i]) != 'nan' and str(work_center[i]) != 'None':  # There has to be a machine given to plan dummy orders
                p = 0  # The priority of the order (lower number is planned/prioritized earlier)
                if not (str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None'):  # If it is a promo order
                    p = 1  # The priority is higher
                if str(orig_document[i]) == 'nan' or str(orig_document[i]) == 'None':  # If it is a listing order
                    p = 2
                if str(in_production[i]) == str(1):  # If the order is already on a machine
                    p = 0  # This is most important
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

    df['Mould'] = mould_df
    df['Priority'] = prio  # Adds the priority of each order to the database
    df['Amount'] = amoun  # Adds the amount of compatible machines to each order
    return sortOrdersDefault(df), wrong


def addAndSortMachines(df_zcs, material):
    """
    :param df_zcs: A dataframe containing the work centers and their cycle times for each material number
    :param material: The item number for which we want the possible workcenters
    :return: A list containing the work centers for the given item (sorted by cycle time)
    """
    workcenters = df_zcs['Work centers'].loc[str(material)]  # A list with the work centers for the material from zcs04
    workcenters2 = df_zcs["('Work center', 'unique')"].loc[str(material)]  # A list with the work centers for the material from historical data
    if type(workcenters2) == float:  # There isn't a workcenter in this list
        workcenters2 = str([])
    workcenters2 = workcenters2.replace('[', '')
    workcenters2 = workcenters2.replace(']', '')
    workcenters2 = workcenters2.replace("'", '')
    workcenters2 = workcenters2.split(' ')
    workcenters = workcenters.replace('[', '')
    workcenters = workcenters.replace(']', '')
    workcenters = workcenters.split(', ')
    cycle = df_zcs['Cycle times'].loc[str(material)]  # A list of the cycle times for workcenters
    cycle = cycle.replace('[', '')
    cycle = cycle.replace(']', '')
    cycle = cycle.split(', ')
    machine = []  # The list containing the work centers for the material ordered by cycle time

    if workcenters != [''] or workcenters2 != []:
        if cycle != ['']:
            if len(workcenters) == len(cycle):  # Only order based on cycle time if we have the cycle time for each work center
                df = pd.DataFrame(columns=['workcenters', 'cycle'])  # Make a dataframe
                for w in range(len(workcenters)):
                    new_row = {'workcenters': str(workcenters[w]), 'cycle': float(cycle[w])}
                    df = df.append(new_row, ignore_index=True)  # adds new row to the dataframe
                df = df.sort_values(by=['cycle'])
                df['Index'] = range(len(workcenters))  # Makes a new index. The other one is jumbled by the sorting
                df = df.set_index('Index')  # (Re)sets the index
                workcenters = df['workcenters']
                for w in range(len(workcenters)):
                    machine.append(workcenters[w])  # Adds the sorted work centers to the output
            else:
                for w in range(len(workcenters)):
                    machine.append(workcenters[w])  # Adds the unsorted work centers to the output
        for w in range(len(workcenters2)):
            if not workcenters2[w] in machine:  # If the workcenter isn't in the output yet
                machine.append(workcenters2[w])  # Adds the work centers to the output
        if machine:  # If the list isn't empty
            return machine
        else:
            return workcenters
    return []


def addcolor(item):
    """
    :param item: The product for which to find the color
    :return: The color of the product
    """
    color = "empty"
    if item[0] == "D" and item[1] == "u" and item[2] == "m" and item[3] == "m" and item[4] == "y":  # It's a dummy
        return color
    elif int(item[5]) == 0:
        if int(item[6]) < 1:
            color = 'transparent'
        elif int(item[6]) < 5:
            color = 'multicolor'
        else:
            color = 'ohne specification'
    elif int(item[5]) == 1:
        if int(item[6]) < 2:
            color = 'white'
        elif int(item[6]) < 6:
            color = 'grey'
        else:
            color = 'silver'
    elif int(item[5]) == 2:
        if int(item[6]) < 5:
            color = 'yellow'
        else:
            color = 'green'
    elif int(item[5]) == 3:
        if int(item[6]) < 5:
            color = 'green'
        else:
            color = 'free'
    elif int(item[5]) == 4:
        if int(item[6]) < 5:
            color = 'red'
        else:
            color = 'orange'
    elif int(item[5]) == 5:
        if int(item[6]) < 5:
            color = 'violet'
        else:
            color = 'pink'
    elif int(item[5]) == 6:
        color = 'blue'
    elif int(item[5]) == 7:
        color = 'free'
    elif int(item[5]) == 8:
        if int(item[6]) < 5:
            color = 'black'
        else:
            color = 'brown'
    elif int(item[5]) == 9:
        color = 'decor'
    return color


def prioritizeColorAndTime(df, old_color):
    """
    :param df: A dataframe with orders, needed on date and color of the orders
    :param old_color: The color of the previous order planned on the machine
    :return: The ordered dataframe
    """
    df_color = pd.read_excel("color.xlsx", header=0)
    df_color = df_color.set_index('Old color')  # A dataframe containing the color severity for each color
    order = df['Order'].astype(str)
    color = df['Color'].astype(str)  # The colors of the orders that still need to be planned

    switch = {
        "x": 0,
        "big": 5,
        "mid": 3,
        "small": 1
    }

    rating = []  # The rating that the product gets for how good it is to be planned next
    if old_color == "empty":  # There was no color known of the product planned before/ there hasn't been a product planned yet
        for i in range(len(order)):  # For each possibly next order
            new_color = color[i]  # Get the (next) orders color
            if str(new_color) == 'empty':
                impact = df_color.loc['none']  # If there is an unknown new color, pretend there is no color
            else:
                impact = df_color.loc[str(new_color)]  # Get the impact from the new color on every other color
            r = 0  # The rating of all the possible impacts the new color can have
            for im in range(len(impact)):
                r += switch.get(impact[im], 5)  # Change the impact to a number rating and add it to r
            rating.append(r)

    else:  # The color of the last planned product is known
        for i in range(len(order)):
            new_color = color[i]  # Get the (next) orders color
            if str(new_color) == 'empty':  # The (next) products color is unknown and we pretend it doesn't have a color
                impact = df_color['none'][old_color]  # Get the impact of the color change
            else:
                impact = df_color[str(new_color)][old_color]  # Get the impact of the color change
            r = 0  # The rating for choosing an order to go next
            r += switch.get(impact, 5)  # Change the impact to a number rating
            rating.append(r)

    df['Color rating'] = rating
    df = df.sort_values(by=['Finish', 'Color rating'])  # First sort by needed on date, then by color rating for least scrap
    df['Index'] = range(len(order))  # Makes a new index. The other one is jumbled
    df = df.set_index('Index')  # (Re)sets the index
    return df


def makeDataframe(products):
    """
    :param products: A list of products to order and plan in the machines
    :return: A dataframe of the products ordered based on the finish date (needed on) and color
    """
    df = pd.DataFrame(columns=['Order', 'Finish', 'Color', 'Mould', 'Insert', 'Prio'])  # Makes the dataframe
    for p in products:
        new_row = {'Order': p, 'Finish': products[p].finish_date, 'Color': addcolor(products[p].id),
                   'Mould': products[p].moulds, 'Insert': products[p].insert, 'Prio': 0}
        df = df.append(new_row, ignore_index=True)  # Adds a new row to the dataframe under the existing rows
    return df


def givePrioDate(n_on, begin_date, end_date, weight_date):
    """
    :param n_on: The needed on date of the order that needs the priority from the date
    :param begin_date: The date that the planning begins
    :param end_date: The date that the planning will end
    :param weight_date: The weight that the needed on date has
    :return: The weight gotten from the needed on date for this particular order
    """
    days = 0
    weight_date /= (end_date-begin_date).days
    n_on = n_on.date()
    if n_on <= begin_date:
        days = begin_date - n_on
    elif n_on <= end_date:
        days = end_date - n_on
    elif n_on > end_date:
        days = n_on - end_date
    return days*weight_date
