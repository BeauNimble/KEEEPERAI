# imports
import datetime


def get_half_time(end_date, begin_date):
    difference_in_days = (datetime.datetime(end_date.year, end_date.month, end_date.day) - datetime.datetime(begin_date.year, begin_date.month, begin_date.day)).days
    return new_time(begin_date.year, begin_date.month, begin_date.day + int(difference_in_days / 2))


def new_time(years, months, days, hours=0, mins=0, secs=0):
    while secs >= 60:
        secs -= 60
        mins += 1
    while mins >= 60:
        mins -= 60
        hours += 1
    while hours >= 24:
        hours -= 24
        days += 1

    beaul = False
    while not beaul:  # Change the total duration of making all combined orders
        try:  # Adds the other product to the total time
            time = datetime.datetime(years, months, days, hours, mins, secs)
            beaul = True
            return time
        except ValueError:
            if months > 12:
                months -= 12
                years += 1
            elif months == 12 and days > (datetime.datetime(years + 1, 1, 1) - datetime.datetime(years, months, 1)).days:
                months -= 11
                years += 1
                days -= (datetime.datetime(years, 1, 1) - datetime.datetime(years - 1, 12, 1)).days
            elif days > (datetime.datetime(years, months + 1, 1) - datetime.datetime(years, months, 1)).days:
                months += 1
                days -= (datetime.datetime(years, months, 1) - datetime.datetime(years, months - 1, 1)).days
            else:
                print('Error no problem, but not changed to time')
                time = datetime.datetime(years, months, days, hours, mins, secs)


def combineOrder(orig_product, new_product):
    orig_product.combined.append(new_product)  # Add the order to the list combining the orders
    new_product.time = 0
    time_sec = orig_product.time.second + new_product.duration.second
    time_min = orig_product.time.minute + new_product.duration.minute
    time_hour = orig_product.time.hour + new_product.duration.hour
    time_day = orig_product.time.day + (new_product.duration.day - 1)
    time_month = orig_product.time.month
    time_year = orig_product.time.year
    if new_product.duration.microsecond > 500:
        time_sec += 1
    orig_product.time = new_time(time_year, time_month, time_day, time_hour, time_min, time_sec)


def withinTime(remainder, time, end_date):
    """
    :param remainder: The time that is already planned on the work center
    :param time: The duration of making all the combined product that we try to plan on the work center
    :param end_date: The date that the planning needs to be done
    :return: True, if the order(s) can be planned in time. Otherwise False
    """
    if remainder > end_date:  # If the machine is already over time
        return False
    # The time to make the products has been added to the time on the work center
    # Still need to add availability to make sure products not over end_date
    time = new_time(remainder.year, remainder.month + (time.month - 1), remainder.day + (time.day - 1), remainder.hour + time.hour, remainder.minute + time.minute, remainder.second + time.second)
    if time < end_date:  # If the order(s) can be planned in time
        return True
    else:
        return False


def changeDuration(product, availability):
    """
    :param product: The product object of which the duration needs to be changed
    :param availability: The availability of the work center that the product will be planned on
    :return: The duration of the product is changed depending on the availability of the machine
    """

    """ Change the time of making the product """
    time_year = product.duration.year
    time_month = product.duration.month
    time_day = ((float(product.duration.day) - 1) * (100 / availability)) / 100
    time_hour = (float(product.duration.hour) * (100 / availability)) / 100
    time_min = int((product.duration.minute * (100 / availability)) / 100)
    time_sec = int((product.duration.second * (100 / availability)) / 100)
    while time_day % 1 > 0:  # It isn't a full day
        time_hour += 60 * (time_day % 1)
        time_day -= time_day % 1
        time_day = int(time_day)
    while time_hour % 1 > 0:  # It isn't a full hour
        time_min += int(60 * (time_hour % 1))
        time_hour -= time_hour % 1
        time_hour = int(time_hour)
    product.duration = new_time(int(time_year), int(time_month), int(time_day) + 1, int(time_hour), int(time_min), int(time_sec))

    if len(product.combined) > 0:  # For the combined order, change the duration
        """ Change the total time of making all combined products """
        time_year = product.time.year
        time_month = product.time.month
        time_day = ((float(product.time.day) - 1) * (100 / availability)) / 100
        time_hour = (float(product.time.hour) * (100 / availability)) / 100
        time_min = int((product.time.minute * (100 / availability)) / 100)
        time_sec = int((product.time.second * (100 / availability)) / 100)
        while time_day % 1 > 0:
            time_hour += 60 * (time_day % 1)
            time_day -= time_day % 1
            time_day = int(time_day)
        while time_hour % 1 > 0:
            time_min += int(60 * (time_hour % 1))
            time_hour -= time_hour % 1
            time_hour = int(time_hour)
        product.time = new_time(int(time_year), int(time_month), int(time_day) + 1, int(time_hour), int(time_min), int(time_sec))

        """ For each combined product, change the time to make it """
        for p in product.combined:  # For each combined order, change the duration
            time_year = p.duration.year
            time_month = p.duration.month
            time_day = ((float(p.duration.day) - 1) * (100 / availability)) / 100
            time_hour = (float(p.duration.hour) * (100 / availability)) / 100
            time_min = int(((p.duration.minute * (100 / availability))) / 100)
            time_sec = int((p.duration.second * (100 / availability)) / 100)
            while time_day % 1 > 0:
                time_hour += 60 * (time_day % 1)
                time_day -= time_day % 1
                time_day = int(time_day)
            while time_hour % 1 > 0:
                time_min += int(60 * (time_hour % 1))
                time_hour -= time_hour % 1
                time_hour = int(time_hour)
            p.duration = new_time(int(time_year), int(time_month), int(time_day) + 1, int(time_hour), int(time_min), int(time_sec))
    # No products are combined, so the combined time is the single product time
    else:
        product.time = product.duration
