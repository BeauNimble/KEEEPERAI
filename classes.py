# Importeren van packages
import pandas as pd
import datetime
from timey import changeDuration, new_time
from ordering import makeDataframe, prioritizeColorAndTime, addcolor
from mould import getInsertChangeTime, getMouldChangeTime, ChangeMould


class TrackingData:
    def __init__(self):
        self.machines_time = pd.DataFrame(columns=['Machine', 'Time Full', 'Time Empty'])  # How much the work centers are empty/full
        self.total_machine_time_full = [0, 0, 0, 0]  # Total time work centers are used [day, hour, minute, second]
        self.total_machine_time_over = [0, 0, 0, 0]  # Total time work centers can still be used [day, hour, minute, second]
        self.items_not_in_database = 0  # The amount of items that aren't in the database
        self.orders_not_planned = 0  # The amount of orders that couldn't be planned
        self.orders_overtime = 0  # The amount of orders that is planned after the end date
        self.orders_planned = 0  # The amount of orders that have been planned
        self.color_severity = [["big", 0], ["medium", 0], ["small", 0], ["none", 0]]  # The amount of times each color severity happens in a change
        self.available_machines = []  # The list of work centers that can be used
        self.conflicted_mould = 0  # The amount of conflicted mould changes


class Product:
    def __init__(self, order, originating_document, idd, time, desc, qty, moulds, insert, machines, finish_date, start, prio):
        self.order = order
        self.originating_document = originating_document
        self.id = idd  # Has the item number of the product
        self.duration = time  # The duration of making the product
        self.time = time  # The duration of making the combined orders
        self.description = desc
        self.quantity = qty
        self.moulds = moulds
        self.insert = insert  # List of inserts for this order
        self.finish_date = finish_date  # The needed on date
        self.machines = machines  # The work centers that can produce the product
        self.combined = []  # The list of combined orders
        self.color_impact = "none"  # The impact of planning this product
        self.scheduled = False  # Has the order been scheduled on a work center yet
        self.start = start  # Is the order already being produced in a work center
        self.priority = prio


class Machine:
    def __init__(self, idd, availability, start_date):
        self.id = idd  # The number of the work center
        self.availability = availability  # How much of the time of the work center is still available
        self.m_date = start_date  # Date in the planning
        self.remainder = start_date  # How much time is planned (of the available time)
        self.use_time = datetime.timedelta(0) # Total time that the machine is in use, just making the product
        self.total_production_time = datetime.timedelta(0) # production time + mould time + insert time, not yet implemented
        self.products = {}  # A dict of the orders that still need to be planned on the work center
        self.schedule = []  # Schedule of all planned products [[product(object), begin, end, duration],…]
        self.old_mould = 'nan'  # The last mould on the work center
        self.old_insert = 'nan'  # The last inserts on the work center

    def first(self, product):
        """
        :param product: The order to schedule on the work center
        :return: The order is scheduled as the first order on the work center
        """
        begin = self.m_date  # The date that the planning starts
        end = new_time(self.m_date.year, self.m_date.month + (product.duration.month - 1), self.m_date.day + (product.duration.day - 1), self.m_date.hour + product.duration.hour, self.m_date.minute + product.duration.minute, self.m_date.second + product.duration.second)
        self.schedule.append([product, begin, end])  # Adds the information for the schedule
        self.old_insert = product.insert  # Changes the last insert used
        self.old_mould = product.moulds  # Changes the last mould used

        if len(product.combined) > 0:  # Schedule orders with the same mould (combined)
            comb_orders = []
            old_insert = product.insert
            product.combined = order_combined_orders(product, addcolor(product.id), old_insert, comb_orders)

            for p in product.combined:  # Each combined order is planned
                add_time = 0
                if len(self.old_insert) == len(product.insert):  # The inserts could be the same (same number of inserts)
                    bo = True
                    for ins in self.old_insert:
                        if not ins in product.insert:
                            bo = False
                    if not bo:  # The inserts are different
                        add_time = getInsertChangeTime(p.moulds)
                        self.old_insert = p.insert
                else:
                    add_time = getInsertChangeTime(p.moulds)
                    self.old_insert = p.insert
                begin = new_time(end.year, end.month, end.day, end.hour, end.minute + add_time, end.second)
                end = new_time(begin.year, begin.month, begin.day + (p.duration.day - 1), begin.hour + p.duration.hour, begin.minute + p.duration.minute, begin.second + p.duration.second)
                self.schedule.append([p, begin, end])  # Information for the schedule
        self.m_date = end

    def add(self, product):
        """
        :param product: The order that will be added to the machine
        :return: Adds the order to the work center to be scheduled
        """
        product.scheduled = True  # Sets the order as scheduled
        self.products[product.id] = product  # Adds the order to the work center
        changeDuration(product, self.availability)  # Changes the duration of making the order based on the availability of the work center
        add_time = getMouldChangeTime(product.moulds)
        total_time = new_time(self.remainder.year, self.remainder.month, self.remainder.day + (product.time.day - 1), self.remainder.hour + product.time.hour, self.remainder.minute + product.time.minute + add_time, self.remainder.second + product.time.second)

        add_time = 0
        if len(product.combined) > 1:  # There are orders combined
            comb_orders = []
            old_insert = product.insert
            product.combined = order_combined_orders(product, addcolor(product.id), old_insert, comb_orders)

            for p in product.combined:  # For each combined product add the duration and insert change time
                if len(old_insert) == len(p.insert):
                    bo = True
                    for ins in old_insert:
                        if not ins in p.insert:
                            bo = False
                    if not bo:  # There is an insert change
                        add_time += getInsertChangeTime(p.moulds)
                        old_insert = p.insert
                elif len(old_insert) != len(p.insert):  # There is an insert change
                    add_time += getInsertChangeTime(p.moulds)
                    old_insert = p.insert
        self.remainder = new_time(total_time.year, total_time.month, total_time.day, total_time.hour, total_time.minute + add_time, total_time.second)

    def sortProducts(self, mould_changes, track, mould_change_capacity, not_planned, end_date):
        df = makeDataframe(self.products)  # Make a dataframe of the order that need to be planned
        plan = self.sortAndSchedule(df, mould_changes, mould_change_capacity, not_planned, end_date)  # Sort the orders and plan them

        for product in plan:  # Add the impact of the color change for each order
            if product[1] == 0:
                track.color_severity[3][1] += 1
                self.products[product[0]].color_impact = "none"
            elif product[1] == 1:
                track.color_severity[2][1] += 1
                self.products[product[0]].color_impact = "small"
            elif product[1] == 3:
                track.color_severity[1][1] += 1
                self.products[product[0]].color_impact = "medium"
            elif product[1] == 5:
                track.color_severity[0][1] += 1
                self.products[product[0]].color_impact = "big"

    def sortAndSchedule(self, df, mould_changes, mould_change_capacity, not_planned, end_date):
        """
        :param df: A dataframe containg all the orders that need to be planned
        :param mould_changes: The list containing all mould changes
        :param mould_change_capacity: A dict containing the mould change capacity for each day
        :param not_planned:
        :param end_date:
        :return: A List with all products and the order they are planned in. Also plans all orders
        """
        plan = []  # List with products planned in correct order
        old_color = "empty"
        old_mould = "empty"
        old_insert = "empty"
        for i in range(len(self.products)):  # For each order in the dataframe
            df = prioritizeColorAndTime(df, old_color)  # Order the dataframe
            product = df['Order']
            impact = df['Color rating']
            self.scheduleTime(self.products[product[0]], mould_changes, mould_change_capacity, not_planned, end_date)  # Schedule the first order
            plan.append([product[0], impact[0]])
            old_color = df['Color'].iloc[0]  # SEt a new last used color
            old_mould = df['Mould'].iloc[0]  # SEt a new last used mould
            old_insert = df['Insert'].iloc[0]  # SEt new last used inserts
            df = df.drop([0])  # Removes the first row
            df['Index'] = range(len(product) - 1)  # Makes a new index. The other one is jumbled
            df = df.set_index('Index')  # (Re)sets the index
        return plan

    def getUsageTime(self):
        """ Adds up all durations of products that are scheduled in the machine"""
        # reset to zero in case of multiple function calls
        self.use_time = datetime.timedelta(0)

        # for each of the scheduled products,
        # add up the total durations
        # Note: it might be useful to change Produce::duration to timedelta
        for scheduled in self.schedule:
            dt_time = scheduled[0].duration
            product_time = datetime.timedelta(days = dt_time.day-1, hours = dt_time.hour, minutes = dt_time.minute, seconds = dt_time.second)
            self.use_time += product_time

    def scheduleTime(self, product, mould_changes, mould_change_capacity, not_planned, end_date):
        """
        :param product: The order that needs to be scheduled
        :param mould_changes: The list containing all mould changes
        :param mould_change_capacity: A dict containing the mould change capacity for each day
        :param not_planned:
        :param end_date:
        :return: Schedules the order on the work center
        """
        begin = self.m_date  # The current date of the planning
        if begin >= end_date:
            not_planned.append([product.id, "No capacity for order on machine", product.order])
            for p in product.combined:
                not_planned.append([p.id, "No capacity for order on machine in for", p.order])
            return

        """ Schedule mould change if necessary """
        if self.old_mould == 'nan' or str(product.moulds) != self.old_mould:  # There is a mould change
            begin = ChangeMould(self.id, product, self.old_mould, self.m_date, mould_changes, mould_change_capacity)  # Change the mould
            if not begin:  # The mould change couldn't be scheduled in time with the capacity
                not_planned.append([product.id, "No capacity for mould change", product.order])
                for p in product.combined:
                    not_planned.append([p.id, "No capacity for mould change", p.order])
                return
            self.old_mould = str(product.moulds)  # Change the last used mould of the work center

        """ Insert change if necessary """
        if self.schedule != [] and self.old_mould != 'nan' and str(product.moulds) == self.old_mould:  # Same mould
            add_time = 0
            if len(self.old_insert) == len(product.insert):
                bo = True
                for ins in self.old_insert:
                    if not ins in product.insert:
                        bo = False
                if not bo:  # Insert change
                    add_time = getInsertChangeTime(product.moulds)
                    self.old_insert = product.insert
                begin = new_time(begin.year, begin. month, begin.day, begin.hour, begin.minute + add_time, begin.second)
            elif len(self.old_insert) != len(product.insert):  # Insert change
                add_time = getInsertChangeTime(product.moulds)
                self.old_insert = product.insert
                begin = new_time(begin.year, begin. month, begin.day, begin.hour, begin.minute + add_time, begin.second)

        if begin >= end_date:
            not_planned.append([product.id, "No capacity for order on machine", product.order])
            for p in product.combined:
                not_planned.append([p.id, "No capacity for order on machine", p.order])
            return

        end = new_time(begin.year, begin.month, begin.day + (product.duration.day - 1), begin.hour + product.duration.hour, begin.minute + product.duration.minute, begin.second + product.duration.second)
        self.schedule.append([product, begin, end])  # Add order to schedule
        self.m_date = end  # Set time of work center

        if len(product.combined) > 0:  # Has Orders with same mould
            no_capacity = False
            for p in product.combined:  # Schedule each combined order
                if not no_capacity:
                    begin = end

                    """ Insert change if necessary """
                    add_time = 0
                    if len(self.old_insert) == len(p.insert):
                        bo = True
                        for ins in self.old_insert:
                            if not ins in p.insert:
                                bo = False
                        if not bo:  # Insert change
                            add_time = getInsertChangeTime(p.moulds)
                            begin = new_time(end.year, end. month, end.day, end.hour, end.minute + add_time, end.second)
                            self.old_insert = p.insert
                    elif len(self.old_insert) != len(p.insert):  # Insert change
                        add_time = getInsertChangeTime(p.moulds)
                        begin = new_time(end.year, end. month, end.day, end.hour, end.minute + add_time, end.second)
                        self.old_insert = p.insert

                    end = new_time(begin.year, begin. month, begin.day + (p.duration.day - 1), begin.hour + p.duration.hour, begin.minute + p.duration.minute, begin.second + p.duration.second)

                    if end > end_date:
                        no_capacity = True
                        not_planned.append([p.id, "No capacity for order on machine", p.order])
                        end = self.m_date
                    else:
                        self.schedule.append([p, begin, end])  # Add order to schedule
                        self.m_date = end  # Set time of work center
                elif no_capacity:
                    not_planned.append([p.id, "No capacity for order on machine", p.order])

        self.getUsageTime()

def order_combined_orders(product, old_color, old_insert, comb_orders):
    combined_products = product.combined                # List of products with the same mould
    needed_ons = []
    inserts = []
    promo = []
    color = []
    df_color = pd.read_excel("color.xlsx", header=0)
    df_color.set_index('Old color', inplace=True, drop=True)

    if len(combined_products) == 1:
        return [combined_products[0]]

    #This for loop makes lists on which the orders shall be sorted.
    # these products have the same mould and must be ordered on which one is first.
    for prod in product.combined:

        switch = {
            "x": 0,
            "X": 0,
            "big": 5,
            "mid": 3,
            "small": 1
        }

        # sort on needed on date
        needed_ons.append(prod.finish_date)

        # sort on 1)promo, 2)listing
        if str(prod.originating_document) != 'None':
            promo.append(10)
        else:
            promo.append(0)

        # this if-elif statement sorts on inserts
        if len(prod.insert) == len(old_insert):
            inserts_same = True
            for ins in old_insert:
                if not ins in prod.insert:
                    inserts_same = False
            if not inserts_same:
                inserts.append(getInsertChangeTime(prod.moulds))
            elif inserts_same:
                inserts.append(0)
        elif len(prod.insert) != len(old_insert):
            inserts.append(getInsertChangeTime(prod.moulds))

        # this sorts by the color
        if str(old_color) == "empty":  # There was no color known of the product planned before/ there hasn't been a product planned yet
            new_color = addcolor(prod.id)
            if str(new_color) == 'empty':
                impact = df_color.loc['none']  # If there is an unknown new color, pretend there is no color
            else:
                impact = df_color.loc[str(new_color)]  # Get the impact from the new color on every other color
            r = 0  # The rating of all the possible impacts the new color can have
            for im in range(len(impact)):
                r += switch.get(impact[im], 5)  # Change the impact to a number rating and add it to r
        else:  # The color of the last planned product is known
            new_color = addcolor(prod.id)
            if str(new_color) == 'empty':  # The (next) products color is unknown and we pretend it doesn't have a color
                impact = df_color.loc['none'][str(old_color)]  # Get the impact of the color change
            else:
                impact = df_color.loc[str(new_color)][str(old_color)]  # Get the impact of the color change
            r = 0  # The rating for choosing an order to go next
            r += switch.get(impact, 5)  # Change the impact to a number rating
            color.append(r)

    #The actual sorting based on a dataframe
    df_combined_orders = pd.DataFrame({'Date': needed_ons, 'Promo': promo, 'Inserts': inserts, 'Color': color, 'Order': combined_products})
    print("Voor ordering: ")
    for i in range(len(combined_products)):
        print(df_combined_orders['Order'].iloc[i].order, df_combined_orders['Inserts'].iloc[i],
              df_combined_orders['Color'].iloc[i])

    df_combined_orders = df_combined_orders.sort_values(by=['Date', 'Promo', 'Inserts', 'Color'])
    print("Na ordering: ")
    for i in range(len(combined_products)):
        print(df_combined_orders['Order'].iloc[i].order, df_combined_orders['Inserts'].iloc[i], df_combined_orders['Color'].iloc[i])

    first = df_combined_orders['Order'].iloc[0]
    old_insert = first.insert
    old_color = addcolor(first.id)

    #We have chose the next order to schedule, this for loop then removes that item from the combined list.
    #This is of course necessary, otherwise we keep choosing the same item.
    for j in range(len(product.combined)):
        if product.combined[j].order == first.order:
            index = j
    del(product.combined[index])
    first.combined = product.combined

    comb_orders = order_combined_orders(first, old_color, old_insert, comb_orders)
    comb_orders.insert(0, first)
    return comb_orders