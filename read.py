# Importeren van packages
import pandas as pd
from classes import Machine


def zcs():
    """
    :material_info.xlsx: contains for each material number, the cycle time on the different available machines and the material description
    Reads the excel file containing the material info and adds the amount of compatible machines for each product
    :return: A dataframe containing material_info
    """
    df = pd.read_excel('material_info_mould.xlsx', header=0)
    df['Material'] = df['Material'].astype(str)  # Makes the material number a string

    workcenter = df['Work centers']  # The list of compatible machines for each product
    amount = []  # A list of the amount of machines that a product is compatible with
    for i in range(len(workcenter)):
        amount.append(len(workcenter[i]))

    df['Amount'] = amount  # Makes a new column in the dataframe containing amount
    df = df.set_index('Material')  # Sets the index to the material number
    return df


def MachineDf(start_date, track):
    """
    :param start_date: a datetime object containing the date and time when the planning starts
    :param track: Keeps track of how many/which machines are available
    :return: A dictionary containing the machines and their availability
    """
    machines = {}
    df_machines = pd.read_excel('Average_Availability.xlsx', header=0)
    machine = df_machines['Nr maszyny Keeeper'].astype(str)
    from_to = df_machines['available\n to, from']
    status = df_machines['status'].astype(str)
    availability = df_machines['utilization']
    for i in range(len(machine)):  # Check each machine
        if status[i] == 'OK':  # If the status is 'OK', the machine is available for use
            machines[machine[i]] = Machine(machine[i], availability[i], start_date)  # makes the Machine object
            track.available_machines.append(machine[i])
        elif from_to[i] != 'nan' and from_to[i] != 'None':
            # There is a posibility to start planning from/plan untill a certain time
            # This might be added later
            pass
    return machines


def AddInsert(order, df):
    """
    :param order: The order number (from the input)
    :param df: The input as a dataframe
    :return: A list containing the inserts for this order
    """
    inserts = []
    df['Order'] = df['Order'].astype(str)
    df = df.set_index('Order')  # Sets the index to the order number to be able to find the inserts based on the order
    ins1 = str(df.loc[str(order), 'Insert 1 (mould)'])  # For the column containing the first insert
    if ins1 != 'nan' and ins1 != 'None':  # If there is an insert given
        inserts.append(ins1)  # Add the insert to the list of inserts
    ins2 = str(df.loc[str(order), 'Insert 2 (mould)'])  # Repeat for the second insert
    if ins2 != 'nan' and ins2 != 'None':
        inserts.append(ins2)
    ins3 = str(df.loc[str(order), 'Insert 3 (mould)'])
    if ins3 != 'nan' and ins3 != 'None':
        inserts.append(ins3)
    ins4 = str(df.loc[str(order), 'Insert 4 (mould)'])
    if ins4 != 'nan' and ins4 != 'None':
        inserts.append(ins4)
    ins5 = str(df.loc[str(order), 'Insert 5 (mould)'])
    if ins5 != 'nan' and ins5 != 'None':
        inserts.append(ins5)
    return inserts
